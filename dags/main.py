from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datawarehouse.dwh import staging_table, core_table
from data_quality.soda import yt_elt_data_quality

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

#variables
staging_schema = "staging"
core_schema = "core"

# DAG 1: Produce JSON 

with DAG(
    dag_id = "produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",
    catchup=False

) as dag_produce:
    
    # Define the tasks using Python functions
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json = save_to_json(extract_data)
    
    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
        
    )
    
    #dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json >> trigger_update_db

# DAG 2: Update DB
with DAG(
    dag_id = "update_db",
    default_args=default_args,
    description="DAG to process JSON and insert data into both staging and core schemas",
    catchup=False,
    schedule=None,
    

) as dag_update:
    # Define the tasks using Python functions
    update_staging = staging_table()
    update_core = core_table()
    
    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
        
    )
    
    #dependencies
    update_staging >> update_core >> trigger_data_quality
    
    
with DAG(
    dag_id = "data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both staging and core schemas",
    catchup=False,
    schedule=None,

) as dag_quality:
    # Define the tasks using Python functions
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)
    
     #dependencies
    soda_validate_staging >> soda_validate_core