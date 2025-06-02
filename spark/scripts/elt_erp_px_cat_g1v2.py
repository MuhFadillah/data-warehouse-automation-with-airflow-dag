from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Setup SparkSession
spark = SparkSession.builder \
    .appName("ELT_ERP_PX_CAT_G1V2") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# Baca file dari MinIO (raw layer)
df = spark.read.option("header", True).csv("s3a://raw/erp/PX_CAT_G1V2.csv")

# Transformasi
df_transformed = df.select (
    expr("id"),
    expr("cat"),
    expr("subcat"),
    expr("maintenance")
)

# Simpan hasil ke MinIO (clean layer)
df_transformed.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("s3a://clean/erp/PX_CAT_G1V2_clean.csv")

# Stop Spark
spark.stop()
