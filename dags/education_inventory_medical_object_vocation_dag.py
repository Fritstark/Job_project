import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from main_dag import Education, Inventory, Medical, Vacation, Object

# Имя файла Excel
xlsx_file = 'data/education.xlsx'
xlsx_file_2 = 'data/inventory.xlsx'
xlsx_file_3 = 'data/medical.xlsx'
xlsx_file_4 = 'data/object.xlsx'
xlsx_file_5 = 'data/vocation.xlsx'    


def get_db_engine():
    connection = BaseHook.get_connection("admin")
    url = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
    return create_engine(url)

def extract_data_from_excel():
    df = pd.read_excel(xlsx_file)
    df_2 = pd.read_excel(xlsx_file_2)
    df_3 = pd.read_excel(xlsx_file_3)
    df_4 = pd.read_excel(xlsx_file_4)
    df_5 = pd.read_excel(xlsx_file_5)
    return df, df_2, df_3, df_4, df_5

def load_data_to_db():
    df, _, _, _, _ = extract_data_from_excel()
    engine = get_db_engine()
    
    with sessionmaker(bind=engine)() as session:
        for _, row in df.iterrows():
            record = session.query(Education).filter_by(tab_id=row['tab_id']).first()
            if record:
                record.theory = row['theory']
                record.test = row['test']
            else:
                session.add(Education(**row.to_dict()))
        session.commit()
    
def load_data_to_db_2():
    _, df_2, _, _, _ = extract_data_from_excel()
    engine = get_db_engine()
    
    with sessionmaker(bind=engine)() as session:
        for _, row in df_2.iterrows():
            record = session.query(Inventory).filter_by(num=row['num']).first()
            if record:
                record.object = row['object']
                record.property = row['property']
                record.quantity = row['quantity']
                record.post_number = row['post_number']
            else:
                session.add(Inventory(**row.to_dict()))
        session.commit()
        
def load_data_to_db_3():
    _, _, df_3, _, _ = extract_data_from_excel()
    engine = get_db_engine()
    
    with sessionmaker(bind=engine)() as session:
        for _, row in df_3.iterrows():
            row['direction_date'] = pd.to_datetime(row['direction_date']).date()            
            record = session.query(Medical).filter_by(num=row['num']).first()
            if record:
                record.tab_id = row['tab_id']
                record.direction_date = row['direction_date']
                record.result = row['result']
                record.info = row['info']
            else:
                session.add(Medical(**row.to_dict()))
        session.commit()
        
def load_data_to_db_4():
    _, _, _, df_4, _ = extract_data_from_excel()
    engine = get_db_engine()
    
    with sessionmaker(bind=engine)() as session:
        for _, row in df_4.iterrows():
            record = session.query(Object).filter_by(object=row['object']).first()
            if record:
                record.address = row['address']
                record.senior_id = row['senior_id']
            else:
                session.add(Object(**row.to_dict()))
        session.commit()
        
def load_data_to_db_5():
    _, _, _, _, df_5 = extract_data_from_excel()
    engine = get_db_engine()
    
    with sessionmaker(bind=engine)() as session:
        for _, row in df_5.iterrows():
            row['vacation_start_1'] = pd.to_datetime(row['vacation_start_1']).date()
            row['vacation_stop_1'] = pd.to_datetime(row['vacation_stop_1']).date()
            row['vacation_start_2'] = pd.to_datetime(row['vacation_start_2']).date()
            row['vacation_stop_2'] = pd.to_datetime(row['vacation_stop_2']).date()          
            record = session.query(Vacation).filter_by(tab_id=row['tab_id']).first()
            if record:
                record.vacation_start_1 = row['vacation_start_1']
                record.vacation_end_1 = row['vacation_stop_1']
                record.vacation_start_2 = row['vacation_start_2']
                record.vacation_end_2 = row['vacation_stop_2']
            else:
                session.add(Vacation(**row.to_dict()))
        session.commit()

with DAG(
    dag_id='education_inventory_medical_object_vocation_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1
    },
    description='DAG для ETL процесса загрузки данных из Excel в PostgreSQL',
    schedule_interval='0 6 * * 1',
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
    
    task_load_3 = PythonOperator(
        task_id='load_data_3',
        python_callable=load_data_to_db_3
    )
    
    task_load_4 = PythonOperator(
        task_id='load_data_4',
        python_callable=load_data_to_db_4
    )
    
    task_load_5 = PythonOperator(
        task_id='load_data_5',
        python_callable=load_data_to_db_5
    )
    
    task_load >> task_load_2 >> task_load_3 >> task_load_4 >> task_load_5