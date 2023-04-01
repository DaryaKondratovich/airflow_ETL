import pendulum
from airflow.decorators import dag, task
from datetime import timedelta
import json


default_args = {
    'owner': 'darya_kondratovich',
    'retries': 10, 
    'retry_delay': timedelta(minutes=5)
}
@dag(
    'ETL_process_new',
    default_args=default_args,
    description='A simple tutorial ETL',
    schedule='5 * * * *',
    catchup=False,
    start_date=pendulum.datetime(2023, 3, 1, tz='Europe/Moscow')
) 
def etl_process():

    @task()
    def extract_data():
        import requests as r

        url = ('http://universities.hipolabs.com/search?country=Kazakhstan')
        response = r.get(url)
        if response.status_code == 200:
            data = json.loads(response.text)
        print(data)
        return data


    @task()  
    def transform_data(df):
        import pandas as pd

        df = pd.DataFrame(extract_data())
        df2 = df.drop(columns = ['state-province'], axis = 1)
        df2['domains'] = df2['domains'].apply(lambda list: list[0])
        df2['web_pages'] = df2['web_pages'].apply(lambda list: list[0])
        df3 = json.loads(df2)
        return df3


    @task()
    def load_data(df3):
        import psycopg2
        from sqlalchemy import create_engine

        engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
        engine.connect()
        df3.head(n=0).to_sql(name = 'education_kz', con=engine, if_exists='replace')
        return df3.to_sql(name = 'education_kz', con=engine, if_exists='append')

    extract = extract_data()
    transform = transform_data(extract)
    load_data(transform)    

etl_process()

