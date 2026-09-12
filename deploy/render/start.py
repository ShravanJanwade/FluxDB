"""Supervise the private Rust engine and public demo proxy in one Render service."""
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time
from urllib.request import urlopen
from urllib.error import URLError


def configuration(template, token, port):
    # Values are inserted into nginx syntax. Fail closed instead of accepting
    # quotes, newlines, or directives in a credential/environment value.
    if not re.fullmatch(r"[A-Za-z0-9_-]{32,256}", token):
        raise ValueError("FLUXDB_TOKEN must contain 32..256 letters, digits, underscores or hyphens")
    number = int(port)
    if not 1024 <= number <= 65535 or number == 18086:
        raise ValueError("PORT must be 1024..65535 and different from internal port 18086")
    return template.replace("__TOKEN__", token).replace("__PORT__", str(number))


def main():
    root = Path(__file__).resolve().parents[2]
    supplied_token = os.environ.get("FLUXDB_TOKEN", "").strip()
    # Render's generatedValue is normally a safe token, but older Blueprint
    # deployments can leave it unset. Keep the public demo bootable and use a
    # private random token in that case; an operator-provided valid token wins.
    token = supplied_token if re.fullmatch(r"[A-Za-z0-9_-]{32,256}", supplied_token) else secrets.token_hex(32)
    if not supplied_token:
        print("FLUXDB_TOKEN was not supplied; generated an ephemeral admin token for this demo instance.", flush=True)
    elif token != supplied_token:
        print("FLUXDB_TOKEN had an unsafe format; generated an ephemeral admin token. Set a 32+ character token for private CRUD access.", flush=True)
    config = configuration(Path(__file__).with_name("nginx.conf.template").read_text(), token, os.getenv("PORT", "10000"))
    config_path = Path("/tmp/fluxdb-nginx.conf")
    config_path.touch(mode=0o600, exist_ok=True)
    config_path.chmod(0o600)
    config_path.write_text(config)
    env = {**os.environ, "FLUXDB_TOKEN": token, "FLUXDB_ADDR": "127.0.0.1:18086", "FLUXDB_DATA_DIR": str(root / "data"), "GEMINI_API_KEY": ""}
    children = []
    stopping = False

    def stop(_signum=None, _frame=None):
        nonlocal stopping
        stopping = True
        for child in reversed(children):
            if child.poll() is None:
                child.terminate()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        server = subprocess.Popen(["fluxdb"], env=env)
        children.append(server)
        for _ in range(300):
            if stopping:
                return
            if server.poll() is not None:
                raise RuntimeError("Database exited during startup")
            try:
                with urlopen("http://127.0.0.1:18086/health", timeout=1) as response:
                    if response.status == 200:
                        break
            except (URLError, TimeoutError):
                time.sleep(0.2)
        else:
            raise RuntimeError("Database readiness timed out")

        marker = root / "data" / ".demo-seeded"
        if not marker.exists():
            seed = subprocess.Popen([sys.executable, str(root / "scripts/demo.py"), "--url", "http://127.0.0.1:18086", "--seconds", "0"], env=env)
            children.append(seed)
            while seed.poll() is None and not stopping:
                time.sleep(0.1)
            if stopping:
                return
            if seed.returncode != 0:
                raise RuntimeError("Demo initialization failed")
            marker.touch()
        subprocess.run(["nginx", "-t", "-c", str(config_path)], check=True)
        if stopping:
            return
        proxy = subprocess.Popen(["nginx", "-c", str(config_path), "-g", "daemon off;"])
        children.append(proxy)
        print("FluxDB demo ready: public browsing/query, authenticated administration, session Gemini keys only", flush=True)
        while not stopping:
            if server.poll() is not None or proxy.poll() is not None:
                raise RuntimeError("A required service exited")
            time.sleep(0.5)
    finally:
        stop()
        deadline = time.monotonic() + 20
        for child in children:
            try:
                child.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


if __name__ == "__main__":
    main()
