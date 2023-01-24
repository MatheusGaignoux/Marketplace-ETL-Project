import json
import ipaddress

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType
from datetime import datetime, timedelta
from dateutils import relativedelta
from utils import *

credentials = {"user":"docker", "password": "docker"}
spark = sparkSessionBuilder("reports-load-step")

def run_load():
    global spark
    global credentials

    ###############################################################
    last_year = (datetime.today() - relativedelta(years = 1)).year
    
    path = "/mnt/g_layer/sales"
    df_sells = (
        spark
        .read
        .format("delta")
        .load(path)
        .filter(col("year_partition") == last_year)
    )

    (
        df_sells
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres-datamart/datamart")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "last_year_orders")
        .option("user", "docker")
        .option("password", "docker")
        .mode("overwrite")
        .save()
    )

    ###############################################################
    path = "/mnt/g_layer/devices"
    df_sells = (
        spark
        .read
        .format("delta")
        .load(path)
    )

    (
        df_sells
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres-datamart/datamart")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "order_devices")
        .option("user", "docker")
        .option("password", "docker")
        .mode("overwrite")
        .save()
    )

    ###############################################################
    path = "/mnt/g_layer/products"
    df_sells = (
        spark
        .read
        .format("delta")
        .load(path)
    )

    (
        df_sells
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres-datamart/datamart")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "popular_products")
        .option("user", "docker")
        .option("password", "docker")
        .mode("overwrite")
        .save()
    )

    ###############################################################
    spark.stop()

if __name__ == "__main__":
    run_load()