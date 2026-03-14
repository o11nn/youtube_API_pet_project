from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

table = "yt_api"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur
    
def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

def create_schema(schema, conn=None, cur=None):
    allowed_schemas = {"staging", "core"}
    if schema not in allowed_schemas:
        raise ValueError(f"Invalid schema: {schema}")

    own_conn = conn is None or cur is None
    if own_conn:
        conn, cur = get_conn_cursor()

    try:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(schema)
            )
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if own_conn:
            close_conn_cursor(conn, cur)


def create_table(schema, conn=None, cur=None):
    allowed_schemas = {"staging", "core"}
    if schema not in allowed_schemas:
        raise ValueError(f"Invalid schema: {schema}")

    own_conn = conn is None or cur is None
    if own_conn:
        conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );
        """)
    else:  # core
        table_sql = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Type" TEXT NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );
        """)

    try:
        cur.execute(
            table_sql.format(
                sql.Identifier(schema),
                sql.Identifier(table),  # uses your global `table = "yt_api"`
            )
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if own_conn:
            close_conn_cursor(conn, cur)

    
#def get_video_ids(cur, schema):
    
    #cur.execute(f""" SELECT "Video_ID" FROM {schema}.{table} ;""")
    #ids = cur.fetchall() #this will return a LIST of DICTIONARIES
    
    #video_ids = [row["Video_ID"] for row in ids] #retrieving only the values from the dictionary
   # return video_ids 