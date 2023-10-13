from sqlalchemy import create_engine, text
import mysql.connector
import pandas as pd

MYSQL_CONFIG = {
        "user": "admin",
        "password": "admin123",
        "host": "localhost",
        "port": 3307,
        "database": "brazillian-ecommerce",
        }

sql_stm = f"SELECT * FROM product_category_name_translation"

str = "mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(**MYSQL_CONFIG)

engine = create_engine(str)

with engine.connect() as con:
    df = pd.DataFrame(con.execute(text(sql_stm)))

print(df)