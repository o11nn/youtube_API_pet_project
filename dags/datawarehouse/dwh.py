from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, delete_rows
from datawarehouse.data_transformation import transform_data


import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():
    schema = "staging"
    conn, cur = get_conn_cursor()

    try:
        YT_data = load_data()

        for row in YT_data:
            insert_rows(cur, conn, schema, row)  # UPSERT

        cur.execute(f'SELECT "Video_ID" FROM {schema}.{table}')
        table_ids = {r["Video_ID"] for r in cur.fetchall()}

        json_ids = {row["video_id"] for row in YT_data}
        ids_to_delete = table_ids - json_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info("staging table updated")

    finally:
        close_conn_cursor(conn, cur)

            
            
@task
def core_table():
    schema = "core"
    conn, cur = get_conn_cursor()

    try:
        cur.execute(f"SELECT * FROM staging.{table}")
        staging_rows = cur.fetchall()

        for row in staging_rows:
            transformed = transform_data(row)
            insert_rows(cur, conn, schema, transformed)  # UPSERT

        cur.execute(f'SELECT "Video_ID" FROM {schema}.{table}')
        core_ids = {r["Video_ID"] for r in cur.fetchall()}

        staging_ids = {row["Video_ID"] for row in staging_rows}
        ids_to_delete = core_ids - staging_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info("core table updated")

    finally:
        close_conn_cursor(conn, cur)
