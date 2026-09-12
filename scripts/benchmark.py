"""Measured HTTP workload, not an engine-wide performance claim. Leaves its database for inspection."""
import argparse, json, math, os, platform, sys, time, uuid
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk/python"))
from fluxdb import FluxDBClient

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default="http://127.0.0.1:8086"); p.add_argument("--points", type=int, default=10000); p.add_argument("--batch", type=int, default=500)
    args = p.parse_args()
    if args.points <= 0 or not 1 <= args.batch <= 10000: p.error("points must be positive; batch must be 1–10,000")
    client = FluxDBClient(args.url, os.getenv("FLUXDB_TOKEN")); name = "bench_" + uuid.uuid4().hex[:12]
    client.create_database(name); started = time.perf_counter(); latencies = []; timestamp = time.time_ns()
    for offset in range(0, args.points, args.batch):
        points = [{"measurement":"cpu", "tags":{"host":str(i % 10)}, "timestamp":str(timestamp + i), "fields":{"usage":float(i % 100)}} for i in range(offset, min(offset + args.batch, args.points))]
        before = time.perf_counter(); client.write(name, points); latencies.append((time.perf_counter()-before)*1000)
    elapsed = time.perf_counter()-started; queries = []
    for _ in range(20):
        before = time.perf_counter(); client.query(name, "SELECT MEAN(usage) FROM cpu"); queries.append((time.perf_counter()-before)*1000)
    def p95(values): return sorted(values)[math.ceil(len(values)*.95)-1]
    print(json.dumps({"database":name,"platform":platform.platform(),"points":args.points,"batch_size":args.batch,"write_seconds":round(elapsed,3),"http_points_per_second":round(args.points/elapsed),"write_batch_p95_ms":round(p95(latencies),3),"query_p95_ms":round(p95(queries),3),"verified_live_points":client.read(name,limit=1)["total"]}, indent=2))
if __name__ == "__main__": main()
