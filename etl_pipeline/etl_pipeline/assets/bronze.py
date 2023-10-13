import pandas as pd
from dagster import asset, Output, AssetsDefinition


def make_assets(
    table_name: str,
    io_manager_key: str = "minio_io_manager",
    compute_kind: str = "MySQL",
    group_name: str = "bronze"
) -> AssetsDefinition:

    @asset(
        name=f"bronze_{table_name}",
        io_manager_key=io_manager_key,
        compute_kind=compute_kind,
        group_name=group_name,
        required_resource_keys={"mysql_io_manager"},
        key_prefix=["bronze", "ecom"],
    )
    def bronze_table(context) -> Output[pd.DataFrame]:
        # define SQL statement
        sql_stm = f"SELECT * FROM {table_name}"
        # use context with resources mysql_io_manager (defined by required_resource_keys)
        # using extract_data() to retrieve data as Pandas Dataframe
        pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
        # return Pandas Dataframe
        # with metadata information
        return Output(
                        pd_data,
                        metadata={
                                    "table": f"{table_name}",
                                    "records count": len(pd_data),
                                },
                        )
    return bronze_table


#
# @asset(
#     io_manager_key="minio_io_manager",
#     required_resource_keys={"mysql_io_manager"},
#     key_prefix=["bronze", "ecom"],
#     compute_kind="MySQL",
#     group_name="bronze_group"
# )
# def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:
#     # define SQL statement
#     sql_stm = "SELECT * FROM olist_orders_dataset"
#     # use context with resources mysql_io_manager (defined by required_resource_keys)
#     # using extract_data() to retrieve data as Pandas Dataframe
#     pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
#     # return Pandas Dataframe
#     # with metadata information
#     return Output(
#                     pd_data,
#                     metadata={
#                                 "table": "olist_orders_dataset",
#                                 "records count": len(pd_data),
#                             },
#                     )
#
#
# @asset(
#         io_manager_key="minio_io_manager",
#         required_resource_keys={"mysql_io_manager"},
#         key_prefix=["bronze", "ecom"],
#         compute_kind="MySQL",
#         group_name="bronze_group"
# )
# def bronze_product_category_name_translation(context) -> Output[pd.DataFrame]:
#     # define SQL statement
#     sql_stm = "SELECT * FROM product_category_name_translation"
#     # use context with resources mysql_io_manager (defined by required_resource_keys)
#     # using extract_data() to retrieve data as Pandas Dataframe
#     pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
#     # return Pandas Dataframe
#     # with metadata information
#     return Output(
#                     pd_data,
#                     metadata={
#                                 "table": "product_category_name_translation",
#                                 "records count": len(pd_data),
#                             },
#                     )
#
#
# @asset(
#         io_manager_key="minio_io_manager",
#         required_resource_keys={"mysql_io_manager"},
#         key_prefix=["bronze", "ecom"],
#         compute_kind="MySQL",
#         group_name="bronze_group"
# )
# def bronze_olist_products_dataset(context) -> Output[pd.DataFrame]:
#     # define SQL statement
#     sql_stm = "SELECT * FROM olist_products_dataset"
#     # use context with resources mysql_io_manager (defined by required_resource_keys)
#     # using extract_data() to retrieve data as Pandas Dataframe
#     pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
#     # return Pandas Dataframe
#     # with metadata information
#     return Output(
#                     pd_data,
#                     metadata={
#                                 "table": "olist_products_dataset",
#                                 "records count": len(pd_data),
#                             },
#                     )
#
#
# @asset(
#         io_manager_key="minio_io_manager",
#         required_resource_keys={"mysql_io_manager"},
#         key_prefix=["bronze", "ecom"],
#         compute_kind="MySQL",
#         group_name="bronze_group"
# )
# def bronze_olist_order_items_dataset(context) -> Output[pd.DataFrame]:
#     # define SQL statement
#     sql_stm = "SELECT * FROM olist_order_items_dataset"
#     # use context with resources mysql_io_manager (defined by required_resource_keys)
#     # using extract_data() to retrieve data as Pandas Dataframe
#     pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
#     # return Pandas Dataframe
#     # with metadata information
#     return Output(
#                     pd_data,
#                     metadata={
#                                 "table": "olist_order_items_dataset",
#                                 "records count": len(pd_data),
#                             },
#                     )
#
#
# @asset(
#         io_manager_key="minio_io_manager",
#         required_resource_keys={"mysql_io_manager"},
#         key_prefix=["bronze", "ecom"],
#         compute_kind="MySQL",
#         group_name="bronze_group"
# )
# def bronze_olist_order_payments_dataset(context) -> Output[pd.DataFrame]:
#     # define SQL statement
#     sql_stm = "SELECT * FROM olist_order_payments_dataset"
#     # use context with resources mysql_io_manager (defined by required_resource_keys)
#     # using extract_data() to retrieve data as Pandas Dataframe
#     pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
#     # return Pandas Dataframe
#     # with metadata information
#     return Output(
#                     pd_data,
#                     metadata={
#                                 "table": "olist_order_payments_dataset",
#                                 "records count": len(pd_data),
#                             },
#                     )
#
#
# MYSQL_CONFIG = {
#         "host": "localhost",
#         "port": 3307,
#         "database": "BrazillianEcommerce",
#         "user": "admin",
#         "password": "admin",
#         }
#
#
# MINIO_CONFIG = {
#         "endpoint_url": "localhost:9000",
#         "bucket": "warehouse",
#         "aws_access_key_id": "minio",
#         "aws_secret_access_key": "minio123",
#         }
#
#
# #define list of assets and resources for data pipeline
# defs = Definitions(
#     assets=[
#         bronze_olist_orders_dataset,
#         bronze_product_category_name_translation,
#         bronze_olist_products_dataset,
#         bronze_olist_order_items_dataset,
#         bronze_olist_order_payments_dataset
#     ],
#     resources={
#         "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
#         "minio_io_manager": MinIOIOManager(MINIO_CONFIG)
#         },
#     )