"""End-to-end verification against an isolated temporary server, including restart and SDKs."""
import argparse, json, os, secrets, shutil, socket, subprocess, sys, tempfile, time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sdk/python"))
from fluxdb import FluxDBClient, FluxDBError

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    args = parser.parse_args()
    suffix = ".exe" if os.name == "nt" else ""
    binary = args.binary or ROOT / "fluxdb/target/release" / ("fluxdb" + suffix)
    if not binary.exists(): binary = ROOT / "fluxdb/target/debug" / ("fluxdb" + suffix)
    if not binary.exists(): raise RuntimeError("Build the server before running smoke.py")
    runtime = (ROOT / ".runtime").resolve(); runtime.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="smoke-", dir=runtime) as folder:
        directory = Path(folder).resolve()
        assert directory.is_relative_to(runtime), "Temporary cleanup must remain under .runtime"
        with socket.socket() as port_socket:
            port_socket.bind(("127.0.0.1", 0)); port = port_socket.getsockname()[1]
        url = f"http://127.0.0.1:{port}"; token = secrets.token_hex(24)
        env = {**os.environ, "GEMINI_API_KEY": "", "FLUXDB_ADDR": f"127.0.0.1:{port}", "FLUXDB_DATA_DIR": str(directory / "data"), "FLUXDB_TOKEN": token, "FLUXDB_CORS_ORIGINS": "http://127.0.0.1:5173", "FLUXDB_TEST_URL": url}
        log = (directory / "server.log").open("w+")
        process = None
        def start():
            nonlocal process
            process = subprocess.Popen([str(binary.resolve())], env=env, stdout=log, stderr=log, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            for _ in range(100):
                if process.poll() is not None: raise RuntimeError("Server exited during startup; inspect temporary log")
                try:
                    with urlopen(url + "/health", timeout=.5) as response:
                        if response.status == 200: return
                except (URLError, TimeoutError): pass
                time.sleep(.1)
            raise RuntimeError("Server did not become healthy")
        def stop():
            nonlocal process
            if process and process.poll() is None:
                process.terminate()
                try: process.wait(timeout=15)
                except subprocess.TimeoutExpired: process.kill(); process.wait(timeout=5)
        try:
            start(); client = FluxDBClient(url, token)
            try: FluxDBClient(url).databases(); raise AssertionError("Unauthenticated request succeeded")
            except FluxDBError as error: assert error.status == 401
            request = Request(url + "/api/v1/databases", method="OPTIONS", headers={"Origin":"http://127.0.0.1:5173", "Access-Control-Request-Method":"GET", "Access-Control-Request-Headers":"authorization"})
            with urlopen(request) as response: assert response.headers["Access-Control-Allow-Origin"] == "http://127.0.0.1:5173"
            with urlopen(Request(url + "/api/v1/assistant/config", headers={"Authorization": "Bearer " + token})) as response:
                config = json.load(response)
                assert config["configured"] is False and config["mutations"] == "review_required"
            try:
                urlopen(Request(url + "/api/v1/assistant/chat", data=json.dumps({"messages":[{"role":"user","text":"Explain FluxDB"}]}).encode(), headers={"Authorization":"Bearer " + token,"Content-Type":"application/json"}))
                raise AssertionError("Unconfigured assistant accepted request")
            except HTTPError as error:
                assert error.code == 503 and "GEMINI_API_KEY" in json.load(error)["error"]
            client.create_database("smoke")
            timestamp = str(time.time_ns())
            point = {"measurement":"cpu","tags":{"host":"api-01"},"timestamp":timestamp,"fields":{"usage":12.5,"requests":{"integer":"9223372036854775807"},"healthy":True,"message":"hello, world"}}
            assert client.write("smoke",[point])["written"] == 1
            assert client.read("smoke")["points"] == [point]
            assert client.query("smoke", "SELECT COUNT(*) FROM cpu WHERE requests = 9223372036854775807 AND healthy = true")["rows"][0][0] == "1"
            client.write("smoke",[{**point,"fields":{"usage":55.5}}])
            assert client.read("smoke")["points"][0]["fields"]["message"] == "hello, world"
            try: client.write("smoke",[point,{**point,"fields":{}}]); raise AssertionError("Invalid batch accepted")
            except FluxDBError as error: assert error.status == 400
            client.flush("smoke"); client.compact("smoke")
            assert "cpu" in client.schema("smoke")["measurements"]
            assert client.retention("smoke")["seconds"] == 0
            client.set_retention("smoke",86400); assert client.retention("smoke")["seconds"] == 86400
            snapshot = client.export("smoke"); snapshot_path=directory/"snapshot.json";snapshot_path.write_text(json.dumps(snapshot),encoding="utf-8")
            subprocess.run([sys.executable,str(ROOT/"scripts/restore.py"),str(snapshot_path),"--database","restored","--url",url],check=True,env=env)
            stop(); start()
            assert client.read("smoke")["points"][0]["fields"]["usage"] == 55.5
            assert client.read("restored")["total"] == 1
            assert client.query("smoke","SELECT COUNT(*) FROM cpu")["rows"][0][0] == "1"
            assert client.request("GET","/api/v1/openapi.json")["openapi"] == "3.1.0"
            assert client.delete("smoke","cpu",timestamp,timestamp,{"host":"api-01"},exact=True)["deleted"] == 1
            client.compact("smoke"); stop(); start(); assert client.read("smoke")["total"] == 0
            client.drop_database("smoke"); assert "smoke" not in client.databases()
            if shutil.which("node"): subprocess.run(["node",str(ROOT/"scripts/sdk-smoke.mjs")],env=env,check=True)
            print("PASS: authentication, CORS, CRUD, exact integers, atomic validation, SQL, checkpoint, restart, restore, deletion recovery, OpenAPI, and SDKs")
        finally:
            stop(); log.close()
if __name__ == "__main__": main()
