# from pyspark.sql import SparkSession
#
# # Define your ADLS account name, container name, and account key
# adls_account_name = "decauto21"
# adls_container_name = "test"
# account_key = "9Gn7n0tlNXAi7cImLo2zM375TWtCeWYFhJa+vcEXz6/MAgJcNTPn9xzC44EiwfFOnYZkQ1IrEajO+AStFp8xDQ=="
#
# # Path to the JAR files required for ADLS connectivity
# azure_storage = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\azure-storage-8.6.6.jar'
# hadoop_azure = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\hadoop-azure-3.3.1.jar'
# hadoop_azure_datalake = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\hadoop-azure-datalake-3.3.1.jar'  # Make sure you have this JAR
#
# # Combine JAR paths for Spark configuration
# jar_path =  azure_storage + ',' + hadoop_azure + ',' + hadoop_azure_datalake
#
# # Initialize Spark session
# spark = SparkSession.builder.master("local[*]") \
#     .appName("pytest_framework") \
#     .config("spark.jars", jar_path) \
#     .config("spark.driver.extraClassPath", jar_path) \
#     .config("spark.executor.extraClassPath", jar_path) \
#     .config("spark.hadoop.fs.azure.account.key." + adls_account_name + ".dfs.core.windows.net", account_key) \
#     .getOrCreate()
#
# # ADLS path to test folder
# adls_path = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/test_folder/"
#
# # Example: Writing data to ADLS
# df = spark.createDataFrame([("test", 1), ("hello", 2)], ["name", "value"])
# df.write.parquet(adls_path + "test_file.parquet")
#
# # Test read operation from ADLS
# df_read = spark.read.parquet(adls_path + "test_file.parquet")
# df_read.show()
#
# # Stop Spark session after use
# spark.stop()
