import os

from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *



def update_delta_table(spark, update_file_path, delta_table_path, key_column):
    # Read the Delta table
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    original_df = delta_table.toDF()

    # Read the update file
    update_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(update_file_path)

    # Get the column names from both datasets
    original_columns = set(original_df.columns)
    update_columns = set(update_data.columns)

    # Find common columns (excluding the key column) and new columns
    common_columns = list(original_columns.intersection(update_columns) - {key_column})
    new_columns = list(update_columns - original_columns)

    # Add new columns to the Delta table if necessary
    if new_columns:
        print(f"Adding new columns: {new_columns}")
        for new_col in new_columns:
            col_type = update_data.schema[new_col].dataType.simpleString()
            spark.sql(f"ALTER TABLE delta.`{delta_table_path}` ADD COLUMN {new_col} {col_type}")

        # Refresh the delta table after adding new columns
        delta_table = DeltaTable.forPath(spark, delta_table_path)

    # Create a dictionary of column updates for existing and new columns
    update_dict = {column: when(col(f"updates.{column}").isNotNull(), col(f"updates.{column}"))
    .otherwise(col(f"original.{column}"))
                   for column in update_columns if column != key_column}

    # Perform the merge operation
    merge_builder = delta_table.alias("original").merge(
        update_data.alias("updates"),
        f"original.{key_column} = updates.{key_column}"
    )

    # Check if there's anything to update (update_dict is not empty)
    if update_dict:
        merge_builder = merge_builder.whenMatchedUpdate(set=update_dict)

    # Always add the whenNotMatchedInsert clause for inserts
    merge_builder = merge_builder.whenNotMatchedInsert(values={col: f"updates.{col}" for col in update_columns})

    # Execute the merge
    merge_builder.execute()

    print(f"Table updated with data from {update_file_path}")
    print(f"New columns added and populated: {new_columns}")

    # Verify the update
    updated_data = spark.read.format("delta").load(delta_table_path)
    print("Updated table schema:")
    updated_data.printSchema()
    print("Sample of updated data:")
    updated_data.show(5)


MINIO_ENDPOINT = "http://localhost:9000"  # Update as per your setup
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "web33"

jars_dir = os.path.abspath("jars")
hadoop_jar = os.path.join(jars_dir, "hadoop-aws-3.3.4.jar")
aws_jar = os.path.join(jars_dir, "aws-java-sdk-bundle-1.12.782.jar")


spark = SparkSession.builder \
    .appName("MinIO PySpark Connection") \
    .config("spark.jars", f"{hadoop_jar},{aws_jar}") \
    .config("spark.driver.extraClassPath", f"{hadoop_jar}:{aws_jar}") \
    .config("spark.executor.extraClassPath", f"{hadoop_jar}:{aws_jar}") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "io.delta.storage.S3SingleDriverLogStore") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")


# reading the all the csv fiels present in the bucket
df = spark.read.option("header", "true").csv(f"s3a://web3/*.csv")

# some transformations 
# df = df.filter(col("gender").isin(["Female", "Male"]))
# df = df.dropDuplicates(["uid"])
df.show()
df.printSchema()

print("==> dataframe count  ->  ",df.count())

# intial delta table 
df.write.format("delta").mode("overwrite").save(f"s3a://web33/DataLake/tables/ABCD0TABLE")

# to update the delta table 
update_delta_table(spark, "s3a://web3/*.csv", "s3a://web33/DataLake/tables/ABCD0TABLE", "uid")

