import pandas as pd
from dagster import asset, Output, Definitions, AssetIn, AssetOut, multi_asset
from etl_pipeline import MySQLIOManager
from etl_pipeline import MinIOIOManager
from etl_pipeline import PostgreSQLIOManager


@asset(
        io_manager_key="minio_io_manager",
        required_resource_keys={"mysql_io_manager"},
        key_prefix=["bronze", "ecom"],
        compute_kind="MySQL"
        )
def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:
    # define SQL statement
    sql_stm = "SELECT * FROM olist_orders_dataset"
    # use context with resources mysql_io_manager (defined by required_resource_keys)
    # using extract_data() to retrieve data as Pandas Dataframe
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    # return Pandas Dataframe
    # with metadata information
    return Output(
                    pd_data,
                    metadata={
                                "table": "olist_orders_dataset",
                                "records count": len(pd_data),
                            },
                    )


@multi_asset(
    ins={
        "bronze_olist_orders_dataset": AssetIn(
                                        key_prefix=["bronze", "ecom"],
                                        )
        },
    outs={
        "olist_orders_dataset": AssetOut(
                                    io_manager_key="psql_io_manager",
                                    key_prefix=["warehouse", "public"],
                                    metadata={
                                            "primary_keys": [
                                                    "order_id",
                                                    "customer_id",
                                                    ],
                                            "columns": [
                                                    "order_id",
                                                    "customer_id",
                                                    "order_status",
                                                    "order_purchase_timestamp",
                                                    "order_approved_at",
                                                    "order_delivered_carrier_date",
                                                    "order_delivered_customer_date",
                                                    "order_estimated_delivery_date",
                                                ],
                                            },
                                    )
        },
    compute_kind="PostgreSQL"
)
def olist_orders_dataset(bronze_olist_orders_dataset) -> Output[pd.DataFrame]:
    return Output(
                bronze_olist_orders_dataset,
                metadata={
                    "schema": "public",
                    "table": "bronze_olist_orders_dataset",
                    "records counts": len(bronze_olist_orders_dataset),
                },
            )


MYSQL_CONFIG = {
        "host": "localhost",
        "port": 3307,
        "database": "brazillian-ecommerce",
        "user": "admin",
        "password": "admin123",
        }


MINIO_CONFIG = {
        "endpoint_url": "localhost:9000",
        "bucket": "warehouse",
        "aws_access_key_id": "minio",
        "aws_secret_access_key": "minio123",
            }


PSQL_CONFIG = {
        "host": "localhost",
        "port": 5434,
        "database": "brazillian-ecommerce",
        "user": "admin",
        "password": "admin123",
        }


# define list of assets and resources for data pipeline
defs = Definitions(
                    assets=[bronze_olist_orders_dataset, olist_orders_dataset],
                    resources={
                                "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
                                "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
                                "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG, MINIO_CONFIG),
                        },
                    )
