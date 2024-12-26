import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from main_dag import MonthlyCollection

xlsx_file = 'data/monthly_collection.xlsx'

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
            record = session.query(MonthlyCollection).filter_by(num=row['num']).first()
            if record:
                record.sector = row['sector']
                record.tab_id = row['tab_id']
                record.change = row['change']
                record.date = row['date']
                record.info = row['info']
            else:
                session.add(MonthlyCollection(**row.to_dict()))
        session.commit()

with DAG(
    dag_id='monthly_collection_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1
    },
    description='DAG для ETL процесса загрузки данных из Excel в PostgreSQL',
    schedule_interval="0 8 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_db
    )

    task_load