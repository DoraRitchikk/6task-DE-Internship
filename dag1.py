from datetime import datetime
import logging
from sqlite3 import Cursor
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.snowflake.operators.snowflake import SnowflakeHook 
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator 
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

try:

    SNOWFLAKE_CONN_ID = 'snowflake_conn'
    SNOWFLAKE_SCHEMA = 'PUBLIC'
    SNOWFLAKE_WAREHOUSE = 'DWH_01'
    SNOWFLAKE_DATABASE = 'Task6'
    SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
    SNOWFLAKE_SAMPLE_TABLE = 'RAW_TABLE'
    SNOWFLAKE_STAGE_TABLE = 'STAGE_TABLE'
    SNOWLAKE_MASTER_TABLE = "MASTER_TABLE"
    SNOWFLAKE_ACCOUNT = "tb70861"
    SNOWFLAKE_USER = "DARIARITCHIK"
    SNOWFLAKE_PASSWORD = "8885Mi7890"

    queryForInsert = "INSERT INTO RAW_TABLE(id,IOS_App_Id ,Title ,Developer_Name ,Developer_IOS_Id ,IOS_Store_Url ,Seller_Official_Website,Age_Rating,Total_Average_Rating ,Total_Number_of_Ratings,Average_Rating_For_Version,Number_of_Ratings_For_Version,Original_Release_Date,Current_Version_Release_Date,Price_USD,Primary_Genre,All_Genres,Languages,Description) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #Определяем параметры владельца и даты старта дага
    args ={
        'owner': 'Airflow',
        'start_date': datetime(2021,1,1),
    }

    # def get_data():
        
    #     #Перебираем записи в chunk
    #     for chunk in pd.read_csv('folderForCsv/763K_plus_IOS_Apps_Info.csv',chunksize=10**5,index_col=False):
            
    #         chunk = chunk.rename(columns={'_id':'id'})
    #         chunk.columns = chunk.columns.str.upper()
    #         dwhCon = SnowflakeHook(snowflake_conn_id = SNOWFLAKE_CONN_ID).get_conn().cursor()
    #         dwhCon.executemany(queryForInsert,chunk.values.tolist())

    # #Создаём Dag, присваиваем аргументы
    # dag = DAG(
    #     dag_id='FIRST_DAG',
    #     default_args=args,
    #     schedule_interval='@daily',
    #     catchup=False
    # )

    # #Создаём Python таску, присваиваем ей функцию выгрузки данных и даг
    # load_task = PythonOperator(task_id="First",python_callable=get_data,dag=dag)

    # #Вызываем таску для выполнения
    # load_task
    

    #СОЗДАНИЕ RAW_TABLE
    CREATE_RAW_TABLE_SQL_STRING = ( f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE}(_id NVARCHAR(24),IOS_App_Id NUMBER(38,0),Title NVARCHAR(16777216),Developer_Name NVARCHAR(16777216),Developer_IOS_Id FLOAT,IOS_Store_Url NVARCHAR(16777216),Seller_Official_Website NVARCHAR(16777216),Age_Rating NVARCHAR(16777216),Total_Average_Rating FLOAT,Total_Number_of_Ratings FLOAT,Average_Rating_For_Version FLOAT,Number_of_Ratings_For_Version NUMBER(38,0),Original_Release_Date NVARCHAR(16777216),Current_Version_Release_Date NVARCHAR(16777216),Price_USD FLOAT,Primary_Genre NVARCHAR(16777216),All_Genres NVARCHAR(16777216),Languages NVARCHAR(16777216),Description NVARCHAR(16777216) );")

    dagCreateRaw =  DAG(
        "CreateRawTable",
        start_date=datetime(2021, 1, 1),
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['example'],
        catchup=False,) 

    RawTabOperator = SnowflakeOperator(
         task_id='TableOperator',
         sql=CREATE_RAW_TABLE_SQL_STRING,
         warehouse=SNOWFLAKE_WAREHOUSE,
         database=SNOWFLAKE_DATABASE,
         schema=SNOWFLAKE_SCHEMA,
         role=SNOWFLAKE_ROLE,
         dag = dagCreateRaw,
    )

    RawTabOperator

    #СОЗДАНИЕ RAW_STREAM
    CREATE_RAW_STREAM_SQL_STRING = (f"CREATE OR REPLACE STREAM RAW_STREAM ON TABLE RAW_TABLE;")

    dagCreateRawStream =  DAG(
        "CreateRawStream",
        start_date=datetime(2021, 1, 1),
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['example'],
        catchup=False,) 

    RawStraemOperator = SnowflakeOperator(
         task_id='StreamOperator',
         sql=CREATE_RAW_STREAM_SQL_STRING,
         warehouse=SNOWFLAKE_WAREHOUSE,
         database=SNOWFLAKE_DATABASE,
         schema=SNOWFLAKE_SCHEMA,
         role=SNOWFLAKE_ROLE,
         dag = dagCreateRawStream,
    )

    RawStraemOperator

    #СОЗДАНИЕ STAGE_TABLE
    CREATE_STAGE_TABLE_SQL_STRING = ( f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_STAGE_TABLE}(_id BIGINT,IOS_App_Id NUMBER(38,0),Title VARCHAR(16777216),Developer_Name VARCHAR(16777216),Developer_IOS_Id FLOAT,IOS_Store_Url VARCHAR(16777216),Seller_Official_Website VARCHAR(16777216),Age_Rating VARCHAR(16777216),Total_Average_Rating FLOAT,Total_Number_of_Ratings FLOAT,Average_Rating_For_Version FLOAT,Number_of_Ratings_For_Version NUMBER(38,0),Original_Release_Date VARCHAR(16777216),Current_Version_Release_Date VARCHAR(16777216),Price_USD FLOAT,Primary_Genre VARCHAR(16777216),All_Genres VARCHAR(16777216),Languages VARCHAR(16777216),Description VARCHAR(16777216) );")

    dagCreateStage =  DAG(
        "CreateStageTable",
        start_date=datetime(2021, 1, 1),
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['example'],
        catchup=False,) 

    StageTabOperator = SnowflakeOperator(
         task_id='TableOperatorStage',
         sql=CREATE_STAGE_TABLE_SQL_STRING,
         warehouse=SNOWFLAKE_WAREHOUSE,
         database=SNOWFLAKE_DATABASE,
         schema=SNOWFLAKE_SCHEMA,
         role=SNOWFLAKE_ROLE,
         dag = dagCreateStage,
    )

    StageTabOperator

    #СОЗДАНИЕ STAGE_STREAM
    CREATE_STAGE_STREAM_SQL_STRING = (f"CREATE OR REPLACE STREAM STAGE_STREAM ON TABLE STAGE_TABLE;")

    dagCreateStageStream =  DAG(
        "CreateStageStream",
        start_date=datetime(2021, 1, 1),
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['example'],
        catchup=False,) 

    StageStraemOperator = SnowflakeOperator(
         task_id='StreamOperatorStage',
         sql=CREATE_STAGE_STREAM_SQL_STRING,
         warehouse=SNOWFLAKE_WAREHOUSE,
         database=SNOWFLAKE_DATABASE,
         schema=SNOWFLAKE_SCHEMA,
         role=SNOWFLAKE_ROLE,
         dag = dagCreateStageStream,
    )

    StageStraemOperator

    #СОЗДАНИЕ MASTER_TABLE
    CREATE_MASTER_TABLE_SQL_STRING = ( f"CREATE OR REPLACE TRANSIENT TABLE {SNOWLAKE_MASTER_TABLE}(IOS_App_Id NUMBER(38,0),Title VARCHAR(16777216),Developer_Name VARCHAR(16777216),Developer_IOS_Id FLOAT,IOS_Store_Url VARCHAR(16777216),Seller_Official_Website VARCHAR(16777216),Age_Rating VARCHAR(16777216),Total_Average_Rating FLOAT,Total_Number_of_Ratings FLOAT,Average_Rating_For_Version FLOAT,Number_of_Ratings_For_Version NUMBER(38,0),Original_Release_Date VARCHAR(16777216),Current_Version_Release_Date VARCHAR(16777216),Price_USD FLOAT,Primary_Genre VARCHAR(16777216),All_Genres VARCHAR(16777216),Languages VARCHAR(16777216),Description VARCHAR(16777216) );")

    dagCreateMaster =  DAG(
        "CreateMasterTable",
        start_date=datetime(2021, 1, 1),
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['example'],
        catchup=False,) 

    MasterTabOperator = SnowflakeOperator(
         task_id='TableOperatorMaster',
         sql=CREATE_MASTER_TABLE_SQL_STRING,
         warehouse=SNOWFLAKE_WAREHOUSE,
         database=SNOWFLAKE_DATABASE,
         schema=SNOWFLAKE_SCHEMA,
         role=SNOWFLAKE_ROLE,
         dag = dagCreateMaster,
    )

    MasterTabOperator


except Exception as a:
    logging.info(str(a))