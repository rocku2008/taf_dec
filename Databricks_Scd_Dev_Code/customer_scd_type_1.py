# Databricks notebook source
# MAGIC %md
# MAGIC #File to raw load

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# input_file = "customer_data_03.csv"
adls_account_name = "decauto21"
adls_container_name = "test"
key = "9Gn7n0tlNXAi7cImLo2zM375TWtCeWYFhJa+vcEXz6/MAgJcNTPn9xzC44EiwfFOnYZkQ1IrEajO+AStFp8xDQ=="

raw_table = "customers_raw"
bronze_table = "customers_bronze"
silver_table = "customers_silver"

# ADLS file path and credentials
adls_path = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/raw/customer/"
spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)


# Azure SQL Server JDBC configuration
jdbc_url = "jdbc:sqlserver://decserver21.database.windows.net:1433;database=decautodb2"
jdbc_properties = {
    "user": "decadmin",
    "password": "Dharmavaram1@",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}



# Step 1: Read source CSV file from ADLS
source_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("batchid", StringType(), True)
])
source_df = spark.read.csv(adls_path, schema=source_schema, header=True)
source_df.display()
source_batchid = source_df.select("batchid").distinct().collect()[0][0]
print("source batch id", source_batchid)

column_list = source_df.columns

# 1sreeni89898 - hashkey1
# 2sreeni89899 - hashkey2
# 1sreeni89898 - hashkey1

source_df = source_df.withColumn("created_date", current_timestamp()).withColumn("updated_date", current_timestamp()).withColumn("hash_key", sha2(concat_ws("||", *[col for col in column_list]), 256))


print("source df")
source_df.display()
source_df.write.jdbc(url=jdbc_url, table=raw_table, mode="append", properties=jdbc_properties)





# COMMAND ----------

# MAGIC %md
# MAGIC #Raw to bronze load

# COMMAND ----------

from pyspark.sql.functions import upper, lower
raw_df = spark.read.jdbc(url=jdbc_url, table=raw_table, properties=jdbc_properties)

raw_df = raw_df.filter(raw_df.batchid == source_batchid)


print("raw_df after batchid filter")
raw_df.display()

bronze_df = raw_df.dropDuplicates(["hash_key"])
# .withColumn("name", upper(col("name"))).withColumn("email", lower(col("email")))



# Write to Bronze Table
bronze_df.write.jdbc(url=jdbc_url, table=bronze_table, mode="overwrite", properties=jdbc_properties)


# COMMAND ----------

# MAGIC %md
# MAGIC #Bronze to silver load

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://decserver21.database.windows.net:1433;database=decautodb2"
jdbc_properties = {
    "user": "decadmin",
    "password": "Dharmavaram1@",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

bronze_df = spark.read.jdbc(url=jdbc_url, table=bronze_table, properties=jdbc_properties)

silver_df = spark.read.jdbc(url=jdbc_url, table=silver_table, properties=jdbc_properties)

silver_df.write.jdbc(url=jdbc_url, table='customers_silver_backup', mode="overwrite", properties=jdbc_properties)


print("bronze df")
bronze_df.display()
print("silver_df")
silver_df.display()


columns = ['customer_id','name','email','phone','batchid','created_date','updated_date']
updates = bronze_df.join(silver_df.select("customer_id", "created_date","batchid"), on="customer_id", how="inner").drop(bronze_df.created_date,bronze_df.batchid)

print("updates")
updates.display()


silver_not_in_bronze = silver_df.join(bronze_df, on="customer_id", how="left_anti")
print("silver_not_in_bronze")
silver_not_in_bronze.display()

new_records = bronze_df.join(silver_df, on="customer_id", how="left_anti")
print("new_records")
new_records.display()


final_df = updates.select(*columns).union(new_records.select(*columns)).union(silver_not_in_bronze.select(*columns))

final_df.cache()
print("final df")
final_df.display()

final_df.write.jdbc(url=jdbc_url, table=silver_table, mode="overwrite", properties=jdbc_properties)

final_df.display()





# COMMAND ----------

# from pyspark.sql.functions import col, current_timestamp, sha2, concat_ws
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# import os

# # Configs
# adls_account_name = "decautoadls"
# adls_container_name = "test"
# adls_key = "vnH8MP/h4VB5vbfsP1x9rAZ5PiyMkIk5RBPnxCbrAjupr7GXMiCv0fHDuySVA036WYaKQDVXcMzz+AStHfeBKQ=="
# adls_path = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/raw/customer/"
# log_path = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/metadata/processed_files.txt"

# jdbc_url = "jdbc:sqlserver://decautoserver.database.windows.net:1433;database=decauto"
# jdbc_properties = {
#     "user": "decadmin",
#     "password": "Dharmavaram1@",
#     "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# }
# raw_table = "customers_raw"

# # Set ADLS Key
# spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", adls_key)

# # Schema
# source_schema = StructType([
#     StructField("customer_id", IntegerType(), True),
#     StructField("name", StringType(), True),
#     StructField("email", StringType(), True),
#     StructField("phone", StringType(), True),
#     StructField("batchid", IntegerType(), True)
# ])

# # Read existing processed file list or create if missing
# try:
#     processed_files_df = spark.read.text(log_path)
#     processed_files = [row.value.strip() for row in processed_files_df.collect()]
# except:
#     print("📂 processed_files.txt not found. Creating new one.")
#     spark.createDataFrame([], "string").write.mode("overwrite").text(log_path)
#     processed_files = []

# # Get all files in raw folder
# all_files = dbutils.fs.ls(adls_path)
# csv_files = [f.name for f in all_files if f.name.endswith(".csv")]

# # Filter unprocessed files
# new_files = [f for f in csv_files if f not in processed_files]

# if not new_files:
#     print("✅ No new files to process.")
# else:
#     for file in new_files:
#         file_path = os.path.join(adls_path, file)
#         print(f"🚀 Processing: {file}")

#         df = spark.read.csv(file_path, schema=source_schema, header=True)

#         if df.count() == 0:
#             print(f"⚠️ Empty file skipped: {file}")
#             continue

#         columns = df.columns
#         df = (
#             df.withColumn("created_date", current_timestamp())
#               .withColumn("updated_date", current_timestamp())
#               .withColumn("hash_key", sha2(concat_ws("||", *[col for col in columns]), 256))
#         )

#         df.write.jdbc(url=jdbc_url, table=raw_table, mode="append", properties=jdbc_properties)
#         print(f"✅ Loaded to table: {raw_table}")

#         # Append to processed_files.txt
#         spark.createDataFrame([(file,)], ["value"]).write.mode("append").text(log_path)
#         print(f"📝 Appended {file} to processed_files.txt")


# COMMAND ----------
