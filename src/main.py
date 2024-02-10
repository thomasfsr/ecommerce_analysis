from duckdb import connect, execute
from os import listdir, makedirs, getenv
from os.path import join, exists, basename
from boto3 import client
from dotenv import load_dotenv

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
    conn = connect(database=':memory:')
    for index, file in enumerate(listdir(path)):
        if not exists(f"{output}/{file.split('.')[0]}.parquet"):
            df = f"table{index}"
            conn.execute(f" create table {df} as select * from read_csv('{join(path,file)}',header=True, auto_detect=True)")
            conn.execute(f"copy {df} TO '{output}/{file.split('.')[0]}.parquet' (FORMAT PARQUET)")
    conn.close()


def export_to_s3(folder:str):
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