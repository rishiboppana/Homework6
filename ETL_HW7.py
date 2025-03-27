from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
# import os
# from datetime import timedelta
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# import yfinance as yf

def connection() :
    hook = SnowflakeHook(snowflake_conn_id = '2nd_snowflake' )
    con =  hook.get_conn()

    return con.cursor()

@task
def initialising():
    con = connection()
    con.execute("""create or replace database dev;   """)
    con.execute("create or replace schema raw ;")

    con.execute("""CREATE TABLE IF NOT EXISTS dev.raw.user_session_channel (
        userId int not NULL,
        sessionId varchar(32) primary key,
        channel varchar(32) default 'direct'  
    );""" )

    con.execute("""CREATE TABLE IF NOT EXISTS dev.raw.session_timestamp (
        sessionId varchar(32) primary key,
        ts timestamp )""")
    
@task    
def extract():
    con = connection()
    con.execute("use database dev")
    con.execute("use schema raw")
    con.execute("""
    CREATE OR REPLACE STAGE blob_stage
    url = 's3://s3-geospatial/readonly/'
    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
    """)
    con.close()

@task
def transfer():    
    con = connection()
    con.execute("use database dev")
    con.execute("use schema raw")
    con.execute("""
    COPY INTO user_session_channel
    FROM @dev.raw.blob_stage/user_session_channel.csv;
    """)

@task
def load():
    con = connection()
    con.execute("use database dev")
    con.execute("use schema raw")
    con.execute("""
    COPY INTO session_timestamp
    FROM @dev.raw.blob_stage/session_timestamp.csv;
            """)

with DAG(
    dag_id = 'ETL_HW7',
    catchup=False ,
    start_date=datetime(2024, 3, 1),   
    tags=['ETL'],  
    schedule_interval='0 8 * * *' 
) as dag:

    i = initialising()
    e= extract()
    t = transfer()
    l = load()
    trigger_ELT = TriggerDagRunOperator(
        task_id = 'Trigger_Task',
        trigger_dag_id = 'ELT'
    )

    i>>e>>t>>l>>trigger_ELT