import pandas as pd
from dagster import asset, Output, Definitions, AssetIn, AssetOut, multi_asset


@asset(
    ins={
        "fact_sales": AssetIn(key_prefix=["silver", "ecom"]),
        "dim_products": AssetIn(key_prefix=["silver", "ecom"]),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "ecom"],
    compute_kind="MinIO",
    group_name="gold"
)
def gold_sales_values_by_category(
    fact_sales,
    dim_products
) -> Output[pd.DataFrame]:
    # Lọc dữ liệu có order_status là 'delivered'
    filtered_sales = fact_sales[fact_sales['order_status'] == 'delivered']

    # Chuyển đổi cột order_purchase_timestamp sang dạng datetime
    filtered_sales['order_purchase_timestamp'] = pd.to_datetime(filtered_sales['order_purchase_timestamp'])

    # Nhóm dữ liệu theo ngày và product_id
    grouped_sales = filtered_sales.groupby([filtered_sales['order_purchase_timestamp'].dt.date, 'product_id']).agg({
        'payment_value': 'sum',
        'order_id': 'nunique'
    }).reset_index()

    # Đổi tên các cột trong kết quả nhóm
    grouped_sales.columns = ['daily', 'product_id', 'sales', 'bills']

    # Làm tròn giá trị sales thành 2 chữ số thập phân
    grouped_sales['sales'] = grouped_sales['sales'].round(2)

    # Tạo DataFrame daily_sales_products từ kết quả nhóm
    daily_sales_products = grouped_sales.copy()

    daily_sales_products['daily'] = pd.to_datetime(daily_sales_products['daily'])

    # Kết hợp dữ liệu từ daily_sales_products và dim_products
    merged_data = pd.merge(daily_sales_products, dim_products, on='product_id', how='inner')

    # Tạo cột mới 'monthly' từ cột 'daily' bằng cách chuyển đổi thành chuỗi tháng và năm
    merged_data['monthly'] = merged_data['daily'].dt.strftime('%Y-%m')

    # Tính toán cột 'values_per_bills'
    merged_data['values_per_bills'] = merged_data['sales'] / merged_data['bills']

    # Chọn các cột cần thiết cho DataFrame daily_sales_categories
    daily_sales_categories = merged_data[
        ['daily', 'monthly', 'product_category_name_english', 'sales', 'bills', 'values_per_bills']]

    # Nhóm dữ liệu theo cột 'monthly' và 'product_category_name_english'
    grouped_stats = daily_sales_categories.groupby(['monthly', 'product_category_name_english']).agg({
        'sales': 'sum',
        'bills': 'sum'
    }).reset_index()

    # Tính toán cột 'values_per_bills'
    grouped_stats['values_per_bills'] = grouped_stats['sales'] / grouped_stats['bills']

    # Chọn các cột cần thiết cho DataFrame mới
    monthly_category_stats = grouped_stats[
        ['monthly', 'product_category_name_english', 'sales', 'bills', 'values_per_bills']]

    pd_data = monthly_category_stats

    return Output(
        pd_data,
        metadata={
            "table": "dim_products",
            "records count": len(pd_data),
        },
    )


@multi_asset(
    ins={
        "gold_sales_values_by_category": AssetIn(
            key_prefix=["gold", "ecom"],
        )
    },
    outs={
        "sales_values_by_category": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "table": "sales_values_by_category"
            }
        )
    },
    compute_kind="PostgreSQL",
    group_name="warehouse"
)
def sales_values_by_category(gold_sales_values_by_category) -> Output[pd.DataFrame]:
    return Output(
        gold_sales_values_by_category,
        metadata={
            "schema": "public",
            "table": "bronze_olist_orders_dataset",
            "records counts": len(gold_sales_values_by_category),
        },
    )

