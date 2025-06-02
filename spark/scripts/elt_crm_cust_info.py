from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when, row_number
from pyspark.sql.window import Window

# Setup SparkSession
spark = SparkSession.builder \
    .appName("ELT_CRM_CUST_INFO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Baca file dari MinIO (raw layer)
df = spark.read.option("header", True).csv("s3a://raw/crm/cust_info.csv")

# Buat window
window_spec = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())

# Transformasi
df_transformed = (
    df.filter(col("cst_id").isNotNull())
    .select(
        "cst_id",
        "cst_key",
        trim(col("cst_firstname")).alias("cst_firstname"),
        trim(col("cst_lastname")).alias("cst_lastname"),
        when(upper(trim(col("cst_marital_status"))) == "S", "Single")
            .when(upper(trim(col("cst_marital_status"))) == "M", "Married")
            .otherwise("n/a").alias("cst_marital_status"),
        when(upper(trim(col("cst_gndr"))) == "F", "Female")
            .when(upper(trim(col("cst_gndr"))) == "M", "Male")
            .otherwise("n/a").alias("cst_gndr"),
        "cst_create_date"
    )
    .withColumn("flag_last", row_number().over(window_spec))
    .filter(col("flag_last") == 1)
    .drop("flag_last")
)

# Simpan hasil ke MinIO (clean layer)
df_transformed.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("s3a://clean/crm/cust_info_clean.csv")

# Stop Spark
spark.stop()
