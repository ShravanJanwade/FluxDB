"""Restore a JSON snapshot into a new database, with bounded write batches."""
import argparse, json, os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk/python"))
from fluxdb import FluxDBClient

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--database", required=True)
    parser.add_argument("--url", default="http://127.0.0.1:8086")
    args = parser.parse_args()
    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    points = snapshot["points"]
    if not isinstance(points, list): raise ValueError("Snapshot points must be an array")
    batches, batch, size = [], [], 0
    for point in points:
        length = len(json.dumps(point, allow_nan=False).encode("utf-8")) + 2
        if length > 1_800_000: raise ValueError("A point exceeds the import body limit")
        if size + length > 1_800_000 or len(batch) == 2500:
            batches.append(batch); batch, size = [], 0
        batch.append(point); size += length
    if batch: batches.append(batch)
    client = FluxDBClient(args.url, os.getenv("FLUXDB_TOKEN"))
    client.create_database(args.database)  # Fail on an existing destination.
    for batch in batches: client.write(args.database, batch)
    actual = client.read(args.database, limit=1)["total"]
    if actual != len(points): raise RuntimeError(f"Count mismatch: expected {len(points)}, restored {actual}")
    client.compact(args.database)
    client.set_retention(args.database, snapshot.get("retention_seconds", 0))
    print(f"Restored and verified {actual} points in {args.database}. Retention policy applied.")
if __name__ == "__main__": main()
