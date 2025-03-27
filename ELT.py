from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
# import os
# from datetime import timedelta
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# import yfinance as yf

def connection():
    hook = SnowflakeHook(snowflake_conn_id = '2nd_snowflake' )
    con =  hook.get_conn()

    return con.cursor()


@task    
def extract():
    con = connection()
    con.execute("use database dev")
    con.execute("create schema if not exists analytics")
    con.execute("use schema analytics")
    # con.execute("""
    # CREATE OR REPLACE STAGE analytics_stage
    # url = 's3://s3-geospatial/readonly/'
    # file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
    # """)
    con.execute("""      
    create table if not exists dev.analytics.wau(
    week date,
    count int
    )
    """)
    con.close()

@task
def transfer():
    con = connection()
    # con.execute("use database dev")
    # con.execute("use schema raw")
    # con.execute("""
    #     COPY INTO session_timestamp
    #     FROM @dev.raw.blob_stage/session_timestamp.csv;
    #             """)
    # con.close()

    con.execute("""
    insert into dev.analytics.wau (week,count)
    select date_trunc('week',ts) ,count(distinct userid)
    from dev.analytics.session_summary
    group by 1;
        """)

    con.close()

@task    
def load():
    con = connection()
    con.execute("use database dev")
    con.execute("use schema analytics")

    con.execute("""
        create or replace table session_summary as 
        (
        select a.userid,a.channel,b.ts
        from dev.raw.user_session_channel as a 
        join dev.raw.session_timestamp as b on a.sessionid = b.sessionid
        );""")

    
with DAG(
    dag_id='ELT',  
    start_date=datetime(2024, 3, 1),  
    catchup=False,  
    tags=['ELT'],  
    schedule_interval='0 8 * * *'  
) as dag:
    # con = connection()
    e = extract()
    l = load()
    t = transfer()

    
    e >> l >> t