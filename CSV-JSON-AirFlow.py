#!/usr/bin/env python
# coding: utf-8

# In[1]:



import time
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import psycopg2
from sqlalchemy import create_engine,Table, Column, Integer, String, MetaData,Date
import pandas as pd

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='ID_Anas',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['DE-PSUT'],
) as dag:

    # [START howto_operator_python]
    task1 = BashOperator(
        task_id='importdata',
        bash_command="pip install pymongo",
    )

    def loadPostgreSqlJson(**kwargs):
        host="de_postgres" # use, 172.26.0.2, "localhost" if you access from outside the localnet decompose env 
        database="psql_data_environment"
        user="psql_user"
        password="psql"
        port='5432'
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        tablename="client_list"
        rdf = pd.read_sql_table(
            tablename,
            con=engine
        )
        import json
        jsonfile=rdf.to_json(orient='records')
        with open('data.json', 'w') as file:
            json.dump(jsonfile, file)
        return 'Importing JSON Done Successfully'
        
    task2 = PythonOperator(
        task_id='loadPostgreSqlJson',
        python_callable=loadPostgreSqlJson,
    )
    def loadJsonMongo(**kwargs):
        from pymongo import MongoClient
        import json
        client = MongoClient("mongodb://mongopsql:mongo@de_mongo:27017")

        db = client["Anas-DE"]

        Collection = db["Data-DE"]

        with open('data.json') as file:
            file_data = json.load(file)
            listfile=json.loads(file_data)
            Collection.insert_many(listfile)

        return 'Loading Successfully'

    task3 = PythonOperator(
        task_id='Writing-on-Mongo',
        python_callable=loadJsonMongo,
    )
    task1 >> task2
    task2 >> task3

