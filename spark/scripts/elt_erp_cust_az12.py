from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, substring, length, upper, trim, expr, current_date

# Setup SparkSession
spark = SparkSession.builder \
    .appName("ELT_ERP_CUST_AZ12") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "your username") \
    .config("spark.hadoop.fs.s3a.secret.key", "your password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# read file from MinIO (raw layer)
df = spark.read.option("header", True).csv("s3a://raw/erp/CUST_AZ12.csv")

# Transformation
df_transformed = df.select(
    when(
        col("cid").like("NAS%"),
        expr("SUBSTRING(cid, 4, length(cid))")
    ).otherwise(
        col("cid")
    ).alias("cid"), 

    when(
        col("bdate") > current_date(),
        None
    ).otherwise(
        col("bdate")
    ).alias("bdate"),

    when(
        upper(trim(col("gen"))).isin("F", "FEMALE"),
        "Female"
    ).when(
        upper(trim(col("gen"))).isin("M", "MALE"),
        "Male"
    ).otherwise(
        "n/a"
    ).alias("gen")
)

# Save results to MinIO (clean layer)
df_transformed.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("s3a://clean/erp/CUST_AZ12_clean.csv")

# Stop Spark
spark.stop()
