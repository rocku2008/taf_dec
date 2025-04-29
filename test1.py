# Databricks notebook source
# MAGIC %md
#%md # File to raw load

# COMMAND ----------

from pyspark.sql import SparkSession
azure_storage = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\azure-storage-8.6.6.jar'
hadoop_azure = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\hadoop-azure-3.3.1.jar'
sql_server = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\mssql-jdbc-12.2.0.jre8.jar'
jar_path =  azure_storage + ',' + hadoop_azure + ',' + sql_server
spark = SparkSession.builder.master("local[*]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()
#from pyspark.sql import SparkSession
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
source_df.show()
source_batchid = source_df.select("batchid").distinct().collect()[0][0]
print("source batch id", source_batchid)

column_list = source_df.columns

# 1sreeni89898 - hashkey1
# 2sreeni89899 - hashkey2
# 1sreeni89898 - hashkey1

source_df = source_df.withColumn("created_date", current_timestamp()).withColumn("updated_date", current_timestamp()).withColumn("hash_key", sha2(concat_ws("||", *[col for col in column_list]), 256))


print("source df")
source_df.show()
source_df.write.jdbc(url=jdbc_url, table=raw_table, mode="append", properties=jdbc_properties)











