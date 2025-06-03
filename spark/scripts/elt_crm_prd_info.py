from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, when, substring, coalesce, expr, lead, regexp_replace, length
)
from pyspark.sql.window import Window

# Setup SparkSession
spark = SparkSession.builder \
    .appName("ELT_CRM_PRD_INFO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "your username") \
    .config("spark.hadoop.fs.s3a.secret.key", "your password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read file from MinIO (raw layer)
df = spark.read.option("header", True).csv("s3a://raw/crm/prd_info.csv")

# create window
window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")

# Transformation
df_transformed = (
    df.select(
        "prd_id",
        regexp_replace(substring("prd_key", 1, 5), "-", "_").alias("cat_id"),
        expr("substring(prd_key, 7,length(prd_key))").alias("prd_key"),
        "prd_nm",
        coalesce(col("prd_cost"), expr("0")).alias("prd_cost"),
        when(upper(trim(col("prd_line"))) == "M", "Mountain")
            .when(upper(trim(col("prd_line"))) == "R", "Road")
            .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
            .when(upper(trim(col("prd_line"))) == "T", "Touring")
            .otherwise("n/a")
            .alias("prd_line"),
        col("prd_start_dt").cast("date").alias("prd_start_dt"),
        (lead("prd_start_dt").over(window_spec).cast("date") - expr("INTERVAL 1 day")).alias("prd_end_dt")
    )
    .orderBy("prd_id") 
)

# Save results to MinIO (clean layer)
df_transformed.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("s3a://clean/crm/prd_info_clean.csv")

# Stop Spark
spark.stop()
