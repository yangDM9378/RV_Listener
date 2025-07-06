import json
import sys
import os
import time
from influxdb_client_3 import InfluxDBClient3, Point

# 인자 수신
influx_url = sys.argv[1]
influx_token = sys.argv[2]
influx_org = sys.argv[3]

# 입력 스트림에서 JSON 읽기
data = json.load(sys.stdin)

print(f"[Influx Writer] URL: {influx_url}, ORG: {influx_org}")
# print("====== [Influx Writer] Received Data ======")
# print(json.dumps(data, indent=2, ensure_ascii=False))
# print("===========================================")
# InfluxDB 연결
client = InfluxDBClient3(
    host=influx_url,
    token=influx_token,
    org=influx_org
)

# Point 구성
point = Point(data['measurement']).time(data['timestamp'], write_precision="ms")

for k, v in data.get("tags", {}).items():
    if v is not None:
        point.tag(k, str(v))

for k, v in data.get("fields", {}).items():
    if v is None:
        continue
    elif isinstance(v, str):
        point.field(k, v)
    elif isinstance(v, float):
        point.field(k, float(v))
    else:
        point.field(k, int(v))

# print(f"[Influx Writer] DB: {data['dbName']}, Measurement: {data['measurement']}, Timestamp(ms): {data['timestamp']}")

# 적재 시도
try:
    client.write(database=data["dbName"], record=point)
    print(f"[Influx Writer] InfluxDB v3 store complete")
except Exception as e:
    print(f"[Influx Writer] ERROR during write: {e}")
    error_dir = "./Log/influxdb_store_error"
    os.makedirs(error_dir, exist_ok=True)
    timestamp_str = time.strftime("%Y%m%d_%H%M%S")
    error_file_path = os.path.join(error_dir, f"failed_data_{timestamp_str}.json")
    with open(error_file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    sys.exit(1)
