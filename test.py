from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
from airflow.providers.snowflake.operators.snowflake import SnowflakeHook


SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "DWH_01"
SNOWFLAKE_DATABASE = "Task6"
SNOWFLAKE_ROLE = "ACCOUNTADMIN"

SNOWFLAKE_SAMPLE_TABLE = "RAW_TABLE"
SNOWFLAKE_STAGE_TABLE = "STAGE_TABLE"
SNOWLAKE_MASTER_TABLE = "MASTER_TABLE"
SNOWFLAKE_ACCOUNT = "tb70861"
SNOWFLAKE_USER = "DARIARITCHIK"
SNOWFLAKE_PASSWORD = "8885Mi7890"

# Определяем параметры владельца и даты старта дага
default_args = {
    "owner": "Airflow",
    "start_date": datetime(2021, 1, 1),
}


def check_table(sf_conn):
    sql_query = """
        select * from information_schema.tables
        where table_name = upper('raw_table')
    """
    sf_cur = sf_conn.cursor()
    try:
        sf_cur.execute(sql_query)
        logging.info(sf_cur.fetchall())
    except Exception as eUploadingFailed:
        logging.info(eUploadingFailed)
        raise eUploadingFailed
    finally:
        sf_cur.close()


def generate_query(table_name: str) -> str:
    insert_query = """
    INSERT INTO RAW_TABLE(
                    _id,
                    IOS_App_Id,
                    Title,
                    Developer_Name,
                    Developer_IOS_Id,
                    IOS_Store_Url,
                    Seller_Official_Website,
                    Age_Rating,
                    Total_Average_Rating,
                    Total_Number_of_Ratings,
                    Average_Rating_For_Version,
                    Number_of_Ratings_For_Version,
                    Original_Release_Date,
                    Current_Version_Release_Date,
                    Price_USD,
                    Primary_Genre,
                    All_Genres,
                    Languages,
                    Description)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

    return insert_query


def get_data():
    # Перебираем записи в chunk
    try:
        sf_conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()
    except Exception as eSomethingWrongWithConnection:
        logging.info(eSomethingWrongWithConnection)
        raise eSomethingWrongWithConnection

    check_table(sf_conn=sf_conn)

    sf_cur = sf_conn.cursor()

    try:
        for i in pd.read_csv('folderForCsv/763K_plus_IOS_Apps_Info.csv',chunksize = 10000):

            #b = pd.DataFrame()
            #b = f.drop(labels=['Unnamed: 0'],axis=1)
        
            i[['IOS_App_Id','Developer_IOS_Id','Total_Average_Rating','Total_Number_of_Ratings','Average_Rating_For_Version','Number_of_Ratings_For_Version','Price_USD']] = i[['IOS_App_Id','Developer_IOS_Id','Total_Average_Rating','Total_Number_of_Ratings','Average_Rating_For_Version','Number_of_Ratings_For_Version','Price_USD']].fillna(-1)
            i[['Description','Languages','All_Genres','Primary_Genre','Current_Version_Release_Date','Original_Release_Date','Age_Rating','Seller_Official_Website','IOS_Store_Url','Developer_Name','Title']] = i[['Description','Languages','All_Genres','Primary_Genre','Current_Version_Release_Date','Original_Release_Date','Age_Rating','Seller_Official_Website','IOS_Store_Url','Developer_Name','Title']].fillna("-1")
        

            df_to_list = i.values.tolist()
            sf_cur.executemany(generate_query(table_name=SNOWFLAKE_SAMPLE_TABLE), df_to_list)
            
    except Exception as eUploadingFailed:
        logging.info(eUploadingFailed)
        raise eUploadingFailed
    finally:
        sf_cur.close()
        sf_conn.close()
            #chunk = chunk.replace(' ','')
            #chunk = chunk.replace('\n','',regex=True)
            #chunk[chunk['_id']].fillna("-1")
            #logging.info("number_values_filled")
            #chunk[chunk['Description','Languages','All_Genres','Primary_Genre','Current_Version_Release_Date','Original_Release_Date','Age_Rating','Seller_Official_Website','IOS_Store_Url','Developer_Name','Title']].fillna("-1")
            #logging.info("string_values_filled")
            #'IOS_App_Id','Developer_IOS_Id','Total_Average_Rating','Total_Number_of_Ratings','Average_Rating_For_Version','Number_of_Ratings_For_Version','Price_USD'
            #df = chunk.fillna('-1')

with DAG(dag_id="airflow_to_snowflake",
         default_args=default_args,
         schedule_interval=None,
         max_active_runs=1,
         catchup=False
         ) as dag:

    t0 = DummyOperator(task_id="start_process")

    # Создаём Python таску, присваиваем ей функцию выгрузки данных и даг
    load_task = PythonOperator(task_id="load_task",
                               python_callable=get_data)

    # Вызываем таску для выполнения
    t0 >> load_task
