import json
import ipaddress
import argparse

from pyspark.sql.functions import (col, lit, max as colmax, min as colmin, split, concat, date_format,
                                   to_timestamp, to_date, regexp_extract, when, udf, size)
from pyspark.sql.types import StructType, StructField, IntegerType
from datetime import datetime, timedelta
from delta import DeltaTable
from utils import *

credentials = {"user":"docker", "password": "docker"}
spark = sparkSessionBuilder("orders-transform-step")

def run_transform(date, is_historical):
    global spark
    global credentials
    month = date[5:7]
    year = date[0:4]
    
    ############################################################
    table_path = "/mnt/g_layer/sales"
    schema_path = "/mnt/schemas/sales.json"
    file = open(schema_path, "r")
    schema = json.load(file)

    table_exists = DeltaTable.isDeltaTable(spark, table_path)

    if not table_exists:
        defineTable(spark, table_path, schema)

    orders_predicates = ["1 = 1"]
    if not is_historical:
        orders_predicates = [f"date(OrderDate) >= '{year}-{month}-01'"]

    # Event oriented tables (constant changes over time):

    df_orders = (
        spark
        .read
        .option("driver", "org.postgresql.Driver")
        .jdbc(
            url = "jdbc:postgresql://postgres-b2b/b2b-platform",
            table = "orders",
            predicates = orders_predicates,
            properties = credentials
        )
    )

    row = df_orders.select(colmin("OrderId"), colmax("OrderId")).collect()

    minOrderId = row[0][0]
    maxOrderId = row[0][1] + 1

    df_orderitems = (
        spark
        .read
        .option("driver", "org.postgresql.Driver")
        .jdbc(
            url = "jdbc:postgresql://postgres-b2b/b2b-platform",
            table = "orderitems",
            column = "orderId",
            numPartitions = 1,
            lowerBound = minOrderId,
            upperBound = maxOrderId,
            properties = credentials
        )
        .filter(col("orderId") >= minOrderId)
    )

    # Tables with domain values (not constant changes over time):

    df_catalog = (
        spark
        .read
        .option("driver", "org.postgresql.Driver")
        .jdbc(
            url = "jdbc:postgresql://postgres-b2b/b2b-platform",
            table = "catalog",
            properties = credentials
        )
    )

    cols = [
        col("orderItemId").cast("integer"),
        col("orderId").cast("integer"),
        col("price"),
        col("orderDate"),
        date_format(col("orderDate"), "yyyy").alias("year_partition"),
        date_format(col("orderDate"), "MM").alias("month_partition")
    ]

    df = (
        df_orderitems.alias("orderitems")
        .join(df_orders.alias("order"), on = "OrderId", how = "left")
        .join(df_catalog.alias("catalog"), on = "CatalogId", how = "left")
        .select(*cols)
        .orderBy(col("orderItemId").asc())
    )

    condition = """
        disc.OrderItemId = delta.OrderItemId
    """

    mergeTable(spark, table_path, df, condition)

    ############################################################
    table_path_devices = "/mnt/g_layer/devices"
    schema_path_devices = "/mnt/schemas/devices.json"
    file = open(schema_path_devices, "r")
    schema_devices = json.load(file)

    table_exists = DeltaTable.isDeltaTable(spark, table_path_devices)

    if not table_exists:
        defineTable(spark, table_path_devices, schema_devices, False)

    table_path_products = "/mnt/g_layer/products"
    schema_path_products = "/mnt/schemas/products.json"
    file = open(schema_path_products, "r")
    schema_products = json.load(file)

    table_exists = DeltaTable.isDeltaTable(spark, table_path_products)

    if not table_exists:
        defineTable(spark, table_path_products, schema_products, False)

    path_logs = "/mnt/landing/weblogs"
    if not is_historical:
        path_logs = f"/mnt/landing/weblogs/year={year}/month={month}"
        
    schema_path_weblogs = "/mnt/schemas/weblogs-schema.json"
    file = open(schema_path_weblogs, "r")
    dict_schema = json.load(file)
    schema = StructType.fromJson(dict_schema)

    ipLength = udf(lambda x: int(ipaddress.ip_address(x)), IntegerType())

    df_ipkeyslist = (
        spark
        .read
        .option("driver", "org.postgresql.Driver")
        .jdbc(
            url = "jdbc:postgresql://postgres-b2b/b2b-platform",
            table = "ipsrangelist",
            properties = credentials
        )
        .withColumn("ipNumbers", split(col("iprange"), "\."))
        .select(col("*"), concat(col("ipnumbers")[0], col("ipnumbers")[1]).alias("ipKey"), split(col("iprange"), "-").alias("ipboundary"))
        .distinct()
    )

    cols = [
        col("logs.host").alias("HostIp"),
        col("ips.country").alias("Country"),
        col("logs.username").alias("Username"),
        to_timestamp(concat(col("logs.date"), lit(" "), col("logs.time")), "yyyy-MM-dd HH:mm:ss").alias("VisitedAt"),
        when(col("logs.PageType") == "products", lit("product")).when(col("logs.PageType") == "orders", lit("order")).alias("PageType"),
        split(col("logs.referer"), "/").getItem(4).alias("Id"),
        col("logs.DeviceSpec").cast("string").alias("Device"),
        date_format(col("date"), "yyyy").alias("year_partition"),
        date_format(col("date"), "MM").alias("month_partition")
    ]

    df_weblogs = (
        spark
        .read
        .csv(path_logs, sep = " ", schema = schema)
        .withColumn("pageType", split(col("referer"), "/").getItem(3))
        .withColumn("DeviceSpec", split(regexp_extract(col("useragent"), "\\((.*?)\\)", 1), ";"))
        .withColumn("ipNumbers", split(col("host"), "\."))
        .select(col("*"), concat(col("ipnumbers")[0], col("ipnumbers")[1]).alias("ipKey")).alias("logs")
        .join(df_ipkeyslist.alias("ips"), on = "ipKey", how = "inner")
        .filter(ipLength(col("Host")).between(ipLength(col("IpBoundary")[0]), ipLength(col("IpBoundary")[1])))
        .select(*cols)
    )

    ################################################################################
    cols = [
        col("Username"),
        col("VisitedAt"),
        col("Id").alias("OrderId"),
        col("Device")
    ]

    df_logs_devices = df_weblogs.filter(col("PageType") == "order").select(*cols)

    condition = """
        disc.OrderId = delta.OrderId
    """

    mergeTable(spark, table_path_devices, df_logs_devices, condition)

    ###############################################################################
    df_products = (
        spark
        .read
        .option("driver", "org.postgresql.Driver")
        .jdbc(
            url = "jdbc:postgresql://postgres-b2b/b2b-platform",
            table = "products",
            properties = credentials
        )
    )

    cols = [
        col("HostIp"),
        col("Country"),
        col("VisitedAt"),
        col("Id").alias("ProductId"),
        col("ProductName")    
    ]

    df_logs_products = ( 
        df_weblogs.alias("logs")
        .filter(col("PageType") == "product")
        .join(df_products.alias("prod"), on = col("logs.Id") == col("prod.ProductId"))
        .select(*cols)
        .distinct()
    )

    condition = """
        disc.HostIp = delta.HostIp
        and disc.VisitedAt = delta.VisitedAt
        and disc.ProductId = delta.ProductId
    """

    mergeTable(spark, table_path_products, df_logs_products, condition)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--ExecutionDate", type = str)
    parser.add_argument("-hl", "--IsHistorical", type = int)
    args = parser.parse_args()
    execution_date = args.ExecutionDate
    historical_load = bool(args.IsHistorical)
    run_transform(execution_date, historical_load)