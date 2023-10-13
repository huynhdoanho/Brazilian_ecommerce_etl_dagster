from dagster import Definitions

from etl_pipeline.etl_pipeline.resources.mysql_io_manager import MySQLIOManager
from etl_pipeline.etl_pipeline.resources.minio_io_manager import MinIOIOManager
from etl_pipeline.etl_pipeline.resources.psql_io_manager import PostgreSQLIOManager

from etl_pipeline.etl_pipeline.assets.silver import fact_sales, dim_products

from etl_pipeline.etl_pipeline.assets.gold import gold_sales_values_by_category, sales_values_by_category

from etl_pipeline.etl_pipeline.assets.bronze import make_assets


ls_tables = [
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "product_category_name_translation"
]

factory_assets = [make_assets(key) for key in ls_tables]


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
        "database": "postgres",
        "user": "admin",
        "password": "admin123"
        }


defs = Definitions(
    assets=[
        *factory_assets,
        fact_sales,
        dim_products,
        gold_sales_values_by_category,
        sales_values_by_category
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG, MINIO_CONFIG),
    },
)
