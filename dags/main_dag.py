from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import declarative_base
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

# Инициализация ORM
Base = declarative_base()

# Определение модели таблиц
class Fullname(Base):
    __tablename__ = 'full_name'

    tab_id = Column(Integer, primary_key=True)
    name = Column(String)
    surname = Column(String)
    patronymic = Column(String)
    
class DataUsers(Base):
    __tablename__ = 'data_users'

    tab_id = Column(Integer, ForeignKey('full_name.tab_id'), primary_key=True)
    sector = Column(Integer)
    change = Column(Integer)
    phone = Column(String)
    job_title = Column(String)
    num_ud = Column(String)
    date_birth = Column(Date)
    address = Column(String)     
    
class SickLeave(Base):
    __tablename__ = 'sick_leave'

    num = Column(Integer, primary_key=True)
    tab_id = Column(Integer, ForeignKey('full_name.tab_id'))
    open_date = Column(Date)
    extend_date = Column(Date)
    close_date = Column(Date)
    
class ShiftTable(Base):
    __tablename__ = 'shift_table'

    num = Column(Integer, primary_key=True)
    tab_id = Column(Integer, ForeignKey('full_name.tab_id'))
    sector = Column(Integer)
    object = Column(Integer, ForeignKey('object.object'))
    change = Column(Integer)
    address = Column(String)
    date = Column(Date)
    
class DailyReport(Base):
    __tablename__ = 'daily_report'

    num = Column(Integer, primary_key=True)
    object = Column(Integer, ForeignKey('object.object'))
    report = Column(String)
    result = Column(String)
    address = Column(String)
    date = Column(Date)
    num_workers = Column(Integer)
    
class Education(Base):
    __tablename__ = 'education'

    tab_id = Column(Integer, ForeignKey('full_name.tab_id'), primary_key=True)
    theory = Column(String)
    test = Column(String)

class Inventory(Base):
    __tablename__ = 'inventory'

    num = Column(Integer, primary_key=True)
    object = Column(Integer, ForeignKey('object.object'))
    property = Column(String)
    quantity = Column(Integer)
    post_number = Column(Integer)
    
class Medical(Base):
    __tablename__ = 'medical'

    num = Column(Integer, primary_key=True)
    tab_id = Column(Integer, ForeignKey('full_name.tab_id'))
    direction_date = Column(Date)
    result = Column(String)
    info = Column(String)
    
class MonthlyCollection(Base):
    __tablename__ = 'monthly_collection'

    num = Column(Integer, primary_key=True)
    sector = Column(Integer)
    tab_id = Column(Integer, ForeignKey('full_name.tab_id'))
    change = Column(Integer)
    date = Column(Date)
    info = Column(String)
    
class Vacation(Base):
    __tablename__ = 'vacation'

    tab_id = Column(Integer, ForeignKey('full_name.tab_id'), primary_key=True)
    vacation_start_1 = Column(Date)
    vacation_stop_1 = Column(Date)
    vacation_start_2 = Column(Date)
    vacation_stop_2 = Column(Date)

class Object(Base):
    __tablename__ = 'object'

    object = Column(Integer, primary_key=True)
    address = Column(String)
    senior_id = Column(Integer, ForeignKey('full_name.tab_id'))

# Функция для получения движка базы данных через Airflow Connection
def get_db_engine():
    connection = BaseHook.get_connection("admin")  # Используем подключение Airflow с ID "admin"
    url = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
    engine = create_engine(url)
    Base.metadata.create_all(engine)

# Инициализация DAG
with DAG(
    dag_id='main_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1
    },
    description='DAG для создания таблиц в PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # Таск для извлечения данных
    task_extract = PythonOperator(
        task_id='сreate_tables',
        python_callable=get_db_engine  # Указываем функцию для извлечения
    )
    
    
