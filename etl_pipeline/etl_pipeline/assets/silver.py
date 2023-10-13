import pandas as pd
from dagster import asset, Output, AssetIn


@asset(
    ins={
        "bronze_olist_order_items_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
        "bronze_olist_order_payments_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
        "bronze_olist_orders_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["silver", "ecom"],
    compute_kind="MinIO",
    group_name="silver"
)
def fact_sales(
    bronze_olist_orders_dataset,
    bronze_olist_order_items_dataset,
    bronze_olist_order_payments_dataset
) -> Output[pd.DataFrame]:

    bronze_olist_orders_dataset.columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    bronze_olist_order_items_dataset.columns = [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "created_at",
        "updated_at"
    ]

    bronze_olist_order_payments_dataset.columns = [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    ]

    joined_df = pd.merge(
        bronze_olist_orders_dataset,
        bronze_olist_order_items_dataset,
        on="order_id"
    ).merge(bronze_olist_order_payments_dataset,
            on="order_id"
            )

    pd_data = joined_df[[
        "order_id",
        "customer_id",
        "order_purchase_timestamp",
        "product_id",
        "payment_value",
        "order_status"
    ]]

    return Output(
            pd_data,
            metadata={
                    "table": "fact_sales",
                    "records count": len(pd_data),
            },
    )


@asset(
    ins={
        "bronze_olist_products_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
        "bronze_product_category_name_translation": AssetIn(key_prefix=["bronze", "ecom"]),
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["silver", "ecom"],
    compute_kind="MinIO",
    group_name="silver"
)
def dim_products(
        bronze_olist_products_dataset,
        bronze_product_category_name_translation
) -> Output[pd.DataFrame]:
    bronze_olist_products_dataset.columns = [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]

    bronze_product_category_name_translation.columns = [
        "product_category_name",
        "product_category_name_english"
    ]

    joined_df = pd.merge(bronze_olist_products_dataset, bronze_product_category_name_translation,
                         on="product_category_name")

    pd_data = joined_df[["product_id", "product_category_name_english"]]

    return Output(
            pd_data,
            metadata={
                    "table": "dim_products",
                    "records count": len(pd_data),
            },
    )
