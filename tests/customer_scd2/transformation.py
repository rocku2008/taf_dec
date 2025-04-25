from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.master("local[4]") \
        .appName("pytest_framework") \
        .config("spark.jars", "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\mssql-jdbc-12.2.0.jre8.jar") \
        .config("spark.driver.extraClassPath", "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\mssql-jdbc-12.2.0.jre8.jar") \
        .config("spark.executor.extraClassPath", "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\mssql-jdbc-12.2.0.jre8.jar") \
        .getOrCreate()

adls_account_name = "decauto21"
adls_container_name = "raw"
key = "9Gn7n0tlNXAi7cImLo2zM375TWtCeWYFhJa+vcEXz6/MAgJcNTPn9xzC44EiwfFOnYZkQ1IrEajO+AStFp8xDQ=="
input_file = "customer_data_02.csv"
# ADLS file path and credentials
adls_path = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/customer/{input_file}"
spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)


# Azure SQL Server JDBC configuration
jdbc_url = "jdbc:sqlserver://decserver21.database.windows.net:1433;database=decautodb2"
jdbc_properties = {
    "user": "decadmin",
    "password": "Dharmavaram1@",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


bronze_df = spark.read.jdbc(url=jdbc_url, table='customer_bronze_scd2', properties=jdbc_properties)

silver_df = spark.read.jdbc(url=jdbc_url, table='customer_silver_scd2_backup', properties=jdbc_properties)

print("bronze df")
# bronze_df.display()
print("silver_df")
# silver_df.display()

columns = ['customer_id','name','email','phone','batchid','created_date','updated_date','hash_key','start_date','end_date','history_flag']
updates1 = (bronze_df.join(silver_df.select("customer_id", "created_date","batchid"), on="customer_id", how="left_semi")
            .withColumn('start_date', current_timestamp()).withColumn('end_date',lit('2099-12-31T23:59:59')).
            withColumn('history_flag',lit(False)))

print("updates")
# updates1.display()

updates2 = (silver_df.join(bronze_df.select("customer_id", "created_date","batchid"), on="customer_id", how="left_semi").
            withColumn('end_date',current_timestamp()).withColumn('history_flag',lit(True)))

# updates2.display()

updates = updates1.union(updates2)

silver_not_in_bronze = silver_df.join(bronze_df, on="customer_id", how="left_anti")
print("silver_not_in_bronze")
# silver_not_in_bronze.display()

new_records = bronze_df.join(silver_df, on="customer_id", how="left_anti").withColumn('start_date', current_timestamp()).withColumn('end_date',lit('2099-12-31T23:59:59')).withColumn('history_flag',lit(False))
print("new_records")
#new_records.display()

final_df = updates.select(*columns).union(new_records.select(*columns)).union(silver_not_in_bronze.select(*columns))

final_df.cache()
print("final df")
final_df.show()

final_df.write.jdbc(url=jdbc_url, table='customer_silver_scd2_expected', mode="overwrite", properties=jdbc_properties)