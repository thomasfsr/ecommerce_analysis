from src.main import create_dir, convert_to_parquet, export_to_s3, export_to_postgre, create_db

#path = '/mnt/c/Users/home/Downloads/data'
output = 'parquet_files'

#create_dir(output=output)
#convert_to_parquet(path=path,output=output)
#export_to_s3(output)
#export_to_postgre()
create_db('ecommerce', 'parquet_files','database')