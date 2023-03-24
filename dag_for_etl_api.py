import airflow
from airflow import DAG, XComArg
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import requests as r
import json
import psycopg2
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
engine.connect()

def extract_data(**kwargs):
    ti = kwargs['ti']
    response = r.get('http://universities.hipolabs.com/search?country=Kazakhstan')
    if response.status_code == 200:
        json_data = response.json()
    print(json_data)
    ti.xcom_push(key='dataframe', value=json_data)

    
def transform_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='dataframe', task_ids=['extract_data'])
    df = pd.DataFrame(json_data)
    df2 = df.drop(columns = ['state-province'], axis = 1)
    df2['domains'] = df2['domains'].apply(lambda list: list[0])
    df2['web_pages'] = df2['web_pages'].apply(lambda list: list[0])
    ti.xcom_push(key='dataframe', value=df2)

def load_data(**kwargs):
    ti = kwargs['ti']
    df2 = ti.xcom_pull(key='dataframe', task_ids=['transfotm_data'])
    df2.head(n=0).to_sql(name = 'education_kz', con=engine, if_exists='replace')
    return df2.to_sql(name = 'education_kz', con=engine, if_exists='append')

default_args = {
    'owner': 'darya_kondratovich', # Владелец операции 
    'depend_on__past': False, # Зависимость от прошлых запусков
    'email': ['darya.kondratovich@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 55, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
     

}
with DAG(
    'ETL_process',
    default_args=default_args,
    description='A simple tutorial ETL',
    schedule='@once',
    catchup=False,
    start_date=datetime(2022, 1, 1) # Дата начала выполнения DAG

) as dag:
    etl1 = PythonOperator (task_id="extract_data", python_callable = extract_data)
    etl2 = PythonOperator (task_id="transform_data", python_callable = transform_data)
    etl3 = PythonOperator (task_id="load_data", python_callable = load_data)

etl1 >> etl2 >> etl3  

