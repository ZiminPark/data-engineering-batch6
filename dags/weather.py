import logging
from datetime import datetime
from datetime import timedelta

import requests
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

LATITUDE = 37.622
LONGITUDE = 126.914


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def extract(**context):
    params = context["params"]
    api_key = params["api_key"]
    logging.info(api_key)
    lat, lon = params["lat"], params["lon"]
    api = 'https://api.openweathermap.org/data/2.5/onecall' \
          f'?lat={lat}' \
          f'&lon={lon}' \
          '&exclude=current,minutely,hourly,alert' \
          f'&appid={api_key}' \
          '&units=metric'
    res = requests.get(api)
    execution_date = context['execution_date']
    logging.info(execution_date)
    return res.json()


def transform(**context):
    res = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    return res['daily'][:-1]


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    cur = get_Redshift_connection()
    daily = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")

    sql = f"BEGIN; DELETE FROM {schema}.{table};"
    for day in daily:
        date = datetime.fromtimestamp(day["dt"]).strftime('%Y-%m-%d')
        temp = day['temp']
        values = [f"'{date}'", temp['day'], temp['min'], temp['max']]
        values = ', '.join([str(i) for i in values])
        print(values)
        sql += f"""INSERT INTO {schema}.{table} VALUES ({values});"""
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)


weather_dag = DAG(
    dag_id='weather_dag',
    start_date=datetime(2021, 12, 7),  # 날짜가 미래인 경우 실행이 안됨
    schedule_interval='0 16 * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=True,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    }
)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    params={
        'api_key': Variable.get("open_weather_api_key"),
        'lat': LATITUDE,
        'lon': LONGITUDE,
    },
    provide_context=True,
    dag=weather_dag
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=weather_dag
)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    params={
        'schema': 'kpdpkp',  ## 자신의 스키마로 변경
        'table': 'weather_forecast'
    },
    provide_context=True,
    dag=weather_dag
)

extract >> transform >> load
