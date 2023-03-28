import pendulum
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'darya_kondratovich', 
    'depend_on__past': False, 
    'email': ['darya.kondratovich@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 55, 
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'ETL_process_new',
    default_args=default_args,
    description='A simple tutorial ETL',
    schedule='@once',
    catchup=False,
    start_date=pendulum.datetime(2023, 3, 1, tz='Europe/Moscow')

) as dag:
    @task(task_id='extract_data')
    def extract_data(**kwargs):
        import requests as r
        import json

        url = ('http://universities.hipolabs.com/search?country=Kazakhstan')
        response = r.get(url)
        if response.status_code == 200:
            return response.json()


    @task(task_id='transform_data')  
    def transform_data(df, **kwargs):
        import pandas as pd

        df = pd.DataFrame(extract_data())
        df2 = df.drop(columns = ['state-province'], axis = 1)
        df2['domains'] = df2['domains'].apply(lambda list: list[0])
        df2['web_pages'] = df2['web_pages'].apply(lambda list: list[0])
        return df2


    @task(task_id='load_data')
    def load_data(df2, **kwargs):
        import psycopg2
        from sqlalchemy import create_engine

        engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
        engine.connect()
        df2.head(n=0).to_sql(name = 'education_kz', con=engine, if_exists='replace')
        return df2.to_sql(name = 'education_kz', con=engine, if_exists='append')

    extract = extract_data()
    transform = transform_data(extract)
    load = load_data(transform)    

    extract >> transform >> load

