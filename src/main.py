import duckdb
from os import listdir, makedirs, getenv
from os.path import join, exists, basename
from boto3 import client
from dotenv import load_dotenv
import psycopg
from sqlalchemy import create_engine
import pandas as pd
from time import sleep

def create_dir(output:str):
    """
    Create the directory of the output to latter store the data converted
    """
    if not exists(output):
        makedirs(output)

def convert_to_parquet(path:str, output:str):
    """
    Convert to parquet csv files from a provided path to a provided output directory
    """
    conn = duckdb.connect(database=':memory:')
    for index, file in enumerate(listdir(path)):
        if not exists(f"{output}/{file.split('.')[0]}.parquet"):
            df = f"table{index}"
            conn.execute(f" create table {df} as select * from read_csv('{join(path,file)}',header=True, auto_detect=True)")
            conn.execute(f"copy {df} TO '{output}/{file.split('.')[0]}.parquet' (FORMAT PARQUET)")
    conn.close()


def export_to_s3(folder:str):
    """
    Export the parquet files to the s3 bucket
    """
    load_dotenv()
    aws_access_key_id = getenv('aws_access_key_id')
    aws_secret_access_key = getenv('aws_secret_access_key')
    s3_bucket = 'ecommerce-analysis'

    s3 = client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    try:
        for file in listdir(folder):
            file_path = join(folder, file)
            s3.upload_file(file_path, s3_bucket, file)
            print(
                f"File '{file}' successfully exported to S3 bucket '{s3_bucket}'."
                )
    except Exception as e:
        print(f'Error uploading file to S3: {e}')

def export_to_postgre(parquet_file:str='parquet_files/olist_order_payments_dataset.parquet'):
    """
    Export to the PostgreSQL server in the render webservice
    """
    load_dotenv()
    dbname=getenv('dbname')
    user=getenv('user')
    password=getenv('password')
    host=getenv('host')
    port = getenv('port')

    file = parquet_file
    df = pd.read_parquet(file)
    print(df.dtypes)
    conn = psycopg.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host
    )
    engine = create_engine(f'postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}')
    table_name = 'olist_customers_dataset' 
    df.to_sql(table_name, engine, index=False, if_exists='replace')
    conn.close()

def export_to_postgre(input_dir:str='parquet_files'):
    """
    Export parquet files to the PostgreSQL that is hosted at render.com
    """
    load_dotenv()
    dbname=getenv('dbname')
    user=getenv('user')
    password=getenv('password')
    host=getenv('host')
    port = getenv('port')

    for file in listdir(input_dir):
        df = pd.read_parquet(join(input_dir,file))
        print(df.dtypes)
        conn = psycopg.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host
        )
        engine = create_engine(f'postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}')
        table_name = file.split('.')[0]
        df.to_sql(table_name, engine, index=False, if_exists='replace')
    conn.close()

def create_db(dbname:str, folder:str='database', output:str=None):
    if output:
        create_dir(output)
        conn = duckdb.connect(f'{output}/{dbname}.db')
    else:
        conn = duckdb.connect(f'{dbname}.db')
    for file in listdir(folder):
        filename= file.split('.')[0]
        conn.execute(f" create table {filename} as select * from read_parquet('{join(folder,file)}')")
    conn.close()