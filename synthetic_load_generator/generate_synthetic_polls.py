import json, time
from datetime import datetime, timezone

## Manual load-generation tool for stress-testing the LDP-native Bronze experiment 
## (ldp_pipelines/realtime/bronze.py). Not run by CI.

def generate_synthetic_polls(feed_type: str, mode: str, num_files: int):
    landing_path = f"{BASE_LANDING}/realtime_raw_forldp/{feed_type}/{mode}"
    for i in range(num_files):
        rows = [{
            "gtfs_mode": mode,
            "entity_id": f"STRESS_{i}",
            "poll_timestamp": datetime.now(timezone.utc).isoformat(),
            "raw_json": json.dumps({"vehicle": {"id": f"V{i}"}, "position": {"latitude": -33.9, "longitude": 151.2}, "timestamp": str(int(time.time()))}),
        }]
        payload = "\n".join(json.dumps(r) for r in rows)
        file_name = f"poll_stress_{i:05d}.json"
        dbutils.fs.put(f"{landing_path}/{file_name}", payload, overwrite=True)

generate_synthetic_polls("vehicle_positions", "sydneytrains", num_files=500)