This project aim to extract data from the Kaggle e-commerce dataset Olist.  
It assumes that the data was downloaded direct from the kaggle website.  
There is 4 main functions in this project:  
- Convert the csv files to parquet;  
- Export the parquet files to a S3 bucket;  
- Export the tables to a PostgreSQL database in cloud (render.com);  
- Create a OneBigTable from the data to be a data warehouse for queries.  

To install:  
install poetry:  
`pip install poetry`  
clone the repo:  
`git clone https://github.com/thomasfsr/transform_duck.git`  
get in the directory:  
`cd ecommerce_analysis`  
install dependencies:  
`poetry install`  

