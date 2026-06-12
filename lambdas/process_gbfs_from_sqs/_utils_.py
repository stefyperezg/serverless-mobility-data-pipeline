import pandas as pd
import uuid
import json
import os
import csv
import boto3   
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timezone

s3 = boto3.client('s3')

def load_selected_stations():
    file_name = os.environ["SELECTED_STATIONS_FILE"]
    path = f"selected_stations/{file_name}"

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        return {int(row["station_id"]) for row in reader}
    
def safe_serialize(value):
    if value is None:
        return None
    return json.dumps(value, default=str)


def format_filter_stations (stations_status_res, selected_station_ids, tz_local):
    filtered_stations_status = []
    
    for s in stations_status_res:
        try:
            station_id = int(s.get("station_id", -1))
        except (ValueError, TypeError):
            continue

        if station_id in selected_station_ids:

            s['status_uuid'] = str(uuid.uuid4())
            #timestamp local
            last_reported = s.get("last_reported")
            if last_reported:
                dt_utc = datetime.fromtimestamp(last_reported, tz=timezone.utc)
                dt_local = dt_utc.astimezone(tz_local)

                s['vehicle_types_available'] = safe_serialize(s.get('vehicle_types_available'))
                s['vehicle_docks_available'] = safe_serialize(s.get('vehicle_docks_available')) 

                #derived columns for visualization
                s["event_date_local"] = dt_local.date().isoformat()
                s["event_time_local"] = dt_local.isoformat()
                s["event_hour_local"] = dt_local.hour
            else:
                s["event_date_local"] = None
                s["event_time_local"] = None
                s["event_hour_local"] = None

            filtered_stations_status.append(s)

    #sort by station_id
    filtered_stations_status.sort(key=lambda x: int(x["station_id"]))
  
    return filtered_stations_status

def parse_gbfs(raw_data):
    
    station_status_res = json.loads(raw_data)['data']['stations']
   
    return station_status_res


