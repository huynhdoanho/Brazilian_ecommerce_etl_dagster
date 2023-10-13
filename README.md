# Brazilian_ecommerce_etl_dagster

# Dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# Description:
In this project, I will build an ETL pipeline that:
- Extract data from MySQL database.
- Transform with pandas, based on medallion architecture (Bronze > Silver > Gold)
- Load to data warehouse (PostgreSQL)
# Tech:
- Orchestration: Dagster
- Transformation: pandas
- Data warehouse: PostgreSQL
- Data lake: Minio
- Containerize: Docker
# Overview:
![alt text](https://github.com/huynhdoanho/Brazilian_ecommerce_etl_dagster/blob/0ff8bfbe86e189b7cac147524c5ead84be3bb80e/imgs/Screenshot%202023-10-13%20172829.png)
# Run project:
1. Clone the project:
```
git clone https://github.com/huynhdoanho/Brazilian_ecommerce_etl_dagster
```
2. Build
```
docker compose build
```
4. Run
```
docker compose up -d
```
5. Materialize
- <b>Note: Before materializing, make sure that you have imported the Brazillian Ecommerce dataset (I have attached the link above) to data source (MySQL). You can connect to MySQL through port 3307, user: admin, password: admin123, database: brazillian-ecommerce
</b>

- Go to <b> localhost:3001 </b>, our Dagster Asset Lineage will be like this:

![alt text](https://github.com/huynhdoanho/Brazilian_ecommerce_etl_dagster/blob/0ff8bfbe86e189b7cac147524c5ead84be3bb80e/imgs/dags.png)

- Click materialize button, then check PostgreSQL at <i> port: 5434, database: postgres, user: postgres, password: postgres </i> , you can see the result tables.

6. End
```
docker compose down
```
