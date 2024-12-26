import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from main_dag import ShiftTable, SickLeave

# Имя файла Excel
xlsx_file = 'data/shift_table.xlsx'
xlsx_file_2 = 'data/sick_leave.xlsx'


def get_db_engine():
    connection = BaseHook.get_connection("admin")
    url = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
    return create_engine(url)

def extract_data_from_excel():
    df = pd.read_excel(xlsx_file)
    df_2 = pd.read_excel(xlsx_file_2)
    return df, df_2

def load_data_to_db():
    df, _ = extract_data_from_excel()
    engine = get_db_engine()
    
    with sessionmaker(bind=engine)() as session:
        for _, row in df.iterrows():
            row['date'] = pd.to_datetime(row['date']).date()
            record = session.query(ShiftTable).filter_by(num=row['num']).first()
            if record:
                record.tab_id = row['tab_id']
                record.sector = row['sector']
                record.object = row['object']
                record.change = row['change']
                record.address = row['address']
                record.date = row['date']
            else:
                session.add(ShiftTable(**row.to_dict()))
        session.commit()
    
def load_data_to_db_2():
    _, df_2 = extract_data_from_excel()
    engine = get_db_engine()
    
    with sessionmaker(bind=engine)() as session:
        for _, row in df_2.iterrows():
            row['open_date'] = pd.to_datetime(row['open_date']).date()
            row['extend_date'] = pd.to_datetime(row['extend_date']).date()
            row['close_date'] = pd.to_datetime(row['close_date']).date()
            record = session.query(SickLeave).filter_by(num=row['num']).first()
            if record:
                for key in ['tab_id', 'open_date', 'extend_date', 'close_date']:
                    setattr(record, key, row[key])
            else:
                session.add(SickLeave(**row.to_dict()))
        session.commit()

with DAG(
    dag_id='shift_table_sick_leave_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1
    },
    description='DAG для ETL процесса загрузки данных из Excel в PostgreSQL',
    schedule_interval='0 15 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_db
    )

    task_load_2 = PythonOperator(
        task_id='load_data_2',
        python_callable=load_data_to_db_2
    )

    task_load >> task_load_2
