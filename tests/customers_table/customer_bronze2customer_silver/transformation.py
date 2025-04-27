from gitdb.fun import delta_types
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import yaml
import os
taf_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def load_credentials(env="qa"):
    """Load credentials from the centralized YAML file."""
    #taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # credentials_path = taf_path+'/project_config/cred_config.yml'
    #credentials_path= os.path.join(taf_path, "project_config", "cred_config.yml")
    credentials_path = '/Users/admin/PycharmProjects/taf_dec/project_config/cred_config.yml'
    with open(credentials_path, "r") as file:
        credentials = yaml.safe_load(file)
        print(credentials[env])
    return credentials[env]

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

silver_df = spark.read.jdbc(url=jdbc_url, table='customers_silver_backup', properties=jdbc_properties)

columns = ['customer_id','name','email','phone','batchid','created_date','updated_date']
updates = (bronze_df.join(silver_df.select("customer_id", "created_date","batchid"), on="customer_id", how="inner").
           drop(bronze_df.created_date,bronze_df.batchid))

silver_not_in_bronze = silver_df.join(bronze_df, on="customer_id", how="left_anti")

new_records = bronze_df.join(silver_df, on="customer_id", how="left_anti")

final_df = updates.select(*columns).union(new_records.select(*columns)).union(silver_not_in_bronze.select(*columns))

# final_df.cache()
print("final df")
final_df.show()
final_df.write.jdbc(url=jdbc_url, table='customer_silver_expected', mode="overwrite", properties=jdbc_properties)