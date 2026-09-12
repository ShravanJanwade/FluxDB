"""Write explicitly synthetic sample data; dashboard request durations remain real."""
import argparse, math, os, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk/python"))
from fluxdb import FluxDBClient

def batch_at(timestamp, step):
    points = []
    for index, host in enumerate(["api-01", "api-02", "worker-01"]):
        tags = {"host": host, "region": "us-east-1", "source": "synthetic-demo"}
        for measurement, fields in [
            ("cpu", {"usage": round(35 + index * 8 + 18 * math.sin(step / 12 + index), 2), "healthy": True}),
            ("memory", {"used_mb": round(2048 + index * 512 + 200 * math.sin(step / 30), 2)}),
            ("http_requests", {"duration_ms": round(8 + index * 2 + abs(9 * math.sin(step / 17)), 2), "status": {"integer": "200"}}),
        ]:
            points.append({"measurement": measurement, "timestamp": str(timestamp), "tags": tags, "fields": fields})
    return points

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8086")
    parser.add_argument("--database", default="demo_observability")
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    client = FluxDBClient(args.url, os.getenv("FLUXDB_TOKEN"))
    if args.database not in client.databases(): client.create_database(args.database)
    now = time.time_ns()
    for chunk in range(0, 180, 10):
        points = [p for step in range(chunk, chunk + 10) for p in batch_at(now - (180 - step) * 10_000_000_000, step)]
        client.write(args.database, points)
        client.query(args.database, "SELECT MEAN(usage) FROM cpu GROUP BY time('1m'), host")
    end = time.monotonic() + max(0, args.seconds)
    step = 180
    while time.monotonic() < end:
        client.write(args.database, batch_at(time.time_ns(), step))
        client.query(args.database, "SELECT * FROM cpu ORDER BY time DESC LIMIT 50")
        step += 1
        time.sleep(min(2, max(0, end - time.monotonic())))
    client.flush(args.database)
    print(f"Synthetic demo ready: {args.database}; {client.read(args.database, limit=1)['total']} stored points")
if __name__ == "__main__": main()
