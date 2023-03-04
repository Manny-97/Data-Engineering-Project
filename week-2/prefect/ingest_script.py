import os
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True)
def transform_data(df: pd.DataFrame)->pd.DataFrame:
    print(f"pre:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"pre:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3, 
      cache_key_fn=task_input_hash, 
      cache_expiration=timedelta(days=1)
      )
def extract_data(url: str)-> pd.DataFrame:

    output_parquet = "yellow_tripdata_2021-01.parquet"
    print("Getting the data from the source.....")
    os.system(f"wget {url} -O {output_parquet}")
    print("Data successfully downloaded. Now reading the data to pandas.....")
    df = pd.read_parquet(output_parquet)
    return df

@task(log_prints=True, retries=3)
def ingest_data(user: str, password: str, host: str, port: int, db: str, table_name: str, df: pd.DataFrame):

    url = URL.create("postgresql", username=user, password=password, host=host, database=db, port=port)
    engine = create_engine(url)   

    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    query = """
    SELECT *
    FROM yellow_taxi_trips
    LIMIT 10;
    """
    print(pd.read_sql(query, con=engine)
    )

@flow(name="Ingest Flow")
def main(table_name: str):
    user=''
    password=''
    host=''
    port=
    db=''
    parquet_url='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-02.parquet'


    raw_data = extract_data(parquet_url)
    data = transform_data(raw_data)
    ingest_data(user, password, host, port, db, table_name, data)

if __name__ == '__main__':
    
    main('yellow_taxi_trips')

