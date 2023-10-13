import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine
from minio import Minio


class PostgreSQLIOManager(IOManager):

    def __init__(self, config, minio_config):
        self._config = config
        self.minio_config = minio_config
        self.minio_client = Minio(
            minio_config["endpoint_url"],
            access_key=minio_config["aws_access_key_id"],
            secret_key=minio_config["aws_secret_access_key"],
            secure=False
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # insert new data from Pandas Dataframe to PostgreSQL table

        # Create a connection string
        conn_str = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(**self._config)

        # Create a SQLAlchemy engine
        engine = create_engine(conn_str)

        table_name = 'sales_values_by_category'

        # Insert the DataFrame into the table
        obj.to_sql(table_name, con=engine, if_exists='replace', index=False)

        context.log.info(f'DataFrame inserted into PostgreSQL table {table_name}')

    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass