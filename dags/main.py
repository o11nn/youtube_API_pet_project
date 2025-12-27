from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json


#define the local timezone
local_tz = pendulum.timezone("Europe/Warsaw")

#define default arguments for the DAG
default_args = {
    'owner': 'dataengineers',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    'email': 'data@engineers.com',
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(hours=1),
    #'end_date': datetime(2025, 12, 31, tzinfo=local_tz),
}


with DAG(
    dag_id = "produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 4 * * *",
    catchup=False

) as dag:
    # Define the tasks using Python functions
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json = save_to_json(extract_data)
    
    #dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json
    