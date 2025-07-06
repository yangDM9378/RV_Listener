import sys
import json
from influxdb_client_3 import InfluxDBClient3, Point

# 인자 수신
json_path = sys.argv[1]
influx_url = sys.argv[2]
influx_token = sys.argv[3]
influx_org = sys.argv[4]

# JSON 파일 로드
json_path = sys.argv[1]
with open(json_path, 'r') as f:
    data = json.load(f)
print(f"[Influx Writer] URL: {influx_url}, ORG: {influx_org}")

# 로그: 수신된 전체 JSON 내용 출력
print("====== [Influx Writer] send data ======")
print(json.dumps(data, indent=2, ensure_ascii=False))
print("==========================================")

# InfluxDB 연결
client = InfluxDBClient3(
    host=influx_url,
    token=influx_token,
    org=influx_org
)
print("InfluxDB v3 client run complete")

# Point 구성
point = Point(data['measurement']) \
    .time(data['timestamp'], write_precision="ms")

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

# 로그: 적재 정보
print(f"[Influx Writer] DB: {data['dbName']}, Measurement: {data['measurement']}, Timestamp(ms): {data['timestamp']}")

# 적재
client.write(database=data["dbName"], record=point)
print(f"[Influx Writer] InfluxDB v3 store complate")