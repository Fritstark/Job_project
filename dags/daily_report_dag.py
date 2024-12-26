import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from main_dag import DailyReport

xlsx_file = 'data/daily_report.xlsx'

def get_db_engine():
    connection = BaseHook.get_connection("admin")
    url = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
    return create_engine(url)

def extract_data_from_excel():
    df = pd.read_excel(xlsx_file)
    return df

def load_data_to_db():
    df = extract_data_from_excel()
    engine = get_db_engine()
    
    with sessionmaker(bind=engine)() as session:
        for row in df.iterrows():
            row['date'] = pd.to_datetime(row['date']).date()
            record = session.query(DailyReport).filter_by(num=row['num']).first()
            if record:
                record.object = row['object']
                record.report = row['report']
                record.result = row['result']
                record.address = row['address']
                record.date = row['date']
            else:
                session.add(DailyReport(**row.to_dict()))
        session.commit()

with DAG(
    dag_id='daily_report_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1
    },
    description='DAG для ETL процесса загрузки данных из Excel в PostgreSQL',
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_db  # Указываем функцию для загрузки
    )

    task_load
