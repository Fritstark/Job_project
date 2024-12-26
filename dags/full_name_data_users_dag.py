import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from main_dag import Fullname, DataUsers

xlsx_file = 'data/full_name.xlsx'
xlsx_file_2 = 'data/data_users.xlsx'


def get_db_engine():
    connection = BaseHook.get_connection("admin")  # Используем подключение Airflow с ID "admin"
    url = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
    return create_engine(url)

def extract_data_from_excel():
    df = pd.read_excel(xlsx_file)
    df_2 = pd.read_excel(xlsx_file_2)
    return df, df_2

def load_data_to_db():
    df, _ = extract_data_from_excel() #pd.read_csv('/tmp/fn.csv')
    engine = get_db_engine()

    with sessionmaker(bind=engine)() as session:
        for _, row in df.iterrows():
            record = session.query(Fullname).filter_by(tab_id=row['tab_id']).first()
            if record:
                record.name = row['name']
                record.surname = row['surname']
                record.patronymic = row['patronymic']
            else:
                session.add(Fullname(**row.to_dict()))
        session.commit()
    
def load_data_to_db_2():
    _, df_2 = extract_data_from_excel()
    engine = get_db_engine()
    with sessionmaker(bind=engine)() as session:
        for _, row in df_2.iterrows():
            row['date_birth'] = pd.to_datetime(row['date_birth']).date()
            record = session.query(DataUsers).filter_by(tab_id=row['tab_id']).first()
            if record:
                for key in ['sector', 'change', 'phone', 'job_title', 'num_ud', 'date_birth', 'address']:
                    setattr(record, key, row[key])
            else:
                session.add(DataUsers(**row.to_dict()))
        session.commit()

with DAG(
    dag_id='full_name_and_data_users_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1
    },
    description='DAG для ETL процесса загрузки данных из Excel в PostgreSQL',
    schedule_interval='0 0 */3 * *',
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