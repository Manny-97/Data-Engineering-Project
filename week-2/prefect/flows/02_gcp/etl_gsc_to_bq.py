from pathlib import Path
import pandas as pd

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int)->Path:
    """Download data trip from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path)->pd.DataFrame:
    """Data transformation function"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = ['passenger_count'].fillna(0)
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame)->None:
    """Write the transform dataframe to BigQuery"""
    gcp_credential_block = GcpCredentials.load("zoom-gcp-ceds")
    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="INSERT GCP_PROECT_ID",
        credentials=gcp_credential_block.get_credentials_from_service_account(),
        chunksize=100000,
        if_exists="append"
    )
@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load dat to BigQuery data warehouse"""
    color="yellow"
    year=2021
    month=1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()