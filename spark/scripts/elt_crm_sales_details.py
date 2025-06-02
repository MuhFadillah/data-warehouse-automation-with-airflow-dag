from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, length, when, to_date, abs, expr)

# Setup SparkSession
spark = SparkSession.builder \
    .appName("ELT_CRM_SALES_DETAILS") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Baca file dari MinIO (raw layer)
df = spark.read.option("header", True).csv("s3a://raw/crm/sales_details.csv")

# Transformasi 
df_transformed = df.select(
    "sls_ord_num",
    "sls_prd_key",
    "sls_cust_id",
    when(
        (col("sls_order_dt") == 0) | (length(col("sls_order_dt").cast("string")) != 8),
        None
    ).otherwise(
        to_date(col("sls_order_dt").cast("string"), "yyyyMMdd")
    ).alias("sls_order_dt"),
    when(
        (col("sls_ship_dt") == 0) | (length(col("sls_ship_dt").cast("string")) != 8),
        None
    ).otherwise(
        to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd")
    ).alias("sls_ship_dt"),
    when(
        (col("sls_due_dt") == 0) | (length(col("sls_due_dt").cast("string")) != 8),
        None
    ).otherwise(
        to_date(col("sls_due_dt").cast("string"), "yyyyMMdd")
    ).alias("sls_due_dt"),
    when(
        col("sls_sales").isNull() |
        (col("sls_sales") <= 0) |
        (col("sls_sales") != col("sls_quantity") * abs(col("sls_price"))),
        col("sls_quantity") * abs(col("sls_price"))
    ).otherwise(
        col("sls_sales")
    ).alias("sls_sales"),
    "sls_quantity",
    when(
        col("sls_price").isNull() | (col("sls_price") <= 0),
        expr("sls_sales / NULLIF(sls_quantity, 0)")
    ).otherwise(
        col("sls_price")
    ).alias("sls_price")
)

# Simpan hasil ke MinIO (clean layer)
df_transformed.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("s3a://clean/crm/sales_details_clean.csv")

# Stop Spark
spark.stop()