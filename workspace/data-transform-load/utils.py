from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip, DeltaTable

def sparkSessionBuilder(applicationName):
    builder = (
        SparkSession
        .builder
        .master("spark://spark-master:7077")
        .config("spark.jars", "/jars/postgresql-42.5.0.jar,/jars/delta-core_2.12-1.0.0.jar")
        .config("spark.sql.warehouse.dir", "/mnt/warehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
        
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark

def defineTable(spark, table_path, schema, partitioned = True):
    builder = (
        DeltaTable
        .createOrReplace(spark)
        .location(f"{table_path}")
    )

    if partitioned:
        builder = builder.partitionedBy("year_partition", "month_partition")
        
    for metadata in schema["fields"]:
        builder = builder.addColumn(metadata["name"], metadata["type"])

    builder.execute()

def mergeTable(spark, table_path, df, condition):
    df_disc = DeltaTable.forPath(spark, table_path)

    (
        df_disc.alias("disc")
        .merge(df.alias("delta"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
