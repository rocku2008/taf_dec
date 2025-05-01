#from gitdb.fun import delta_types
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import yaml



def load_credentials(env="qa"):
    """Load credentials from the centralized YAML file."""
    credentials_path = "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\project_config\\cred_config.yml"
    with open(credentials_path, "r") as file:
        credentials = yaml.safe_load(file)
        print(credentials[env])
        return credentials[env]

def run_transformation():
    taf_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    azure_storage = os.path.join(taf_path, "jars", "azure-storage-8.6.6.jar")
    hadoop_azure = os.path.join(taf_path, "jars", "hadoop-azure-3.3.1.jar")
    sql_server = os.path.join(taf_path, "jars", "mssql-jdbc-12.2.0.jre8.jar")
    jar_path = azure_storage + ',' + hadoop_azure + ',' + sql_server

    print(azure_storage)
# Initialize Spark session
    spark = SparkSession.builder.master("local[1]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()

    creds_adls = load_credentials()['adls']
    creds_sqlserver = load_credentials()['sqlserver']

    adls_account_name = creds_adls['adls_account_name']
    adls_container_name = creds_adls['adls_container_name']
    key = creds_adls['key']

# ADLS file path and credentials
    adls_path = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/raw/customer/"
    spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)

# Azure SQL Server JDBC configuration
    jdbc_url = creds_sqlserver['url']
    user = creds_sqlserver['user']
    password = creds_sqlserver['password']
    driver = creds_sqlserver['driver']
    print(driver, user,password,jdbc_url)
    jdbc_properties = {
        "user": user,
        "password": password,
        "driver": driver
}


    bronze_df = spark.read.jdbc(url=jdbc_url, table='customers_bronze', properties=jdbc_properties).drop('hash_key')

    silver_df = spark.read.jdbc(url=jdbc_url, table='customers_silver_scd2_backup', properties=jdbc_properties)

    print("bronze df")
# bronze_df.display()
    print("silver_df")
# silver_df.display()

    columns = ['customer_id','name','email','phone','batchid','created_date','updated_date','start_date','end_date','history_flag']
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

#final_df.write.jdbc(url=jdbc_url, table='customers_silver_scd2_expected', mode="overwrite", properties=jdbc_properties)

# Attempt to overwrite data in Azure SQL Server
    try:
        final_df.write.jdbc(
        url=jdbc_url,
        table='customers_silver_scd2_expected',
        mode='overwrite',  # Overwrite mode
        properties=jdbc_properties
        )
        print("Data successfully written to customers_silver_scd2_expected.")
    except Exception as e:
        print(f"Error writing to customers_silver_scd2_expected: {str(e)}")

    spark.catalog.clearCache()
#spark.stop()


# This will allow the transformation to be called in the test file
if __name__ == "__main__":
    run_transformation()