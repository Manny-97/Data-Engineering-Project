import os
from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, parquet_file):
    """The data ingest function. It loads (in parquet format) into the data database."""

    df = pd.read_parquet(parquet_file)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    time_start = time()
    print("Connection established successfully. Now inserting data into the database....")
    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    print("Inserted the headers.....")
    df.to_sql(name=table_name, con=engine, if_exists='append')
    time_end = time()
    print("Time taken to insert data is: %.3f" % (time_end - time_start))
