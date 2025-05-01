# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# import yaml
#
# # Load credentials (as per your existing code)
# def load_credentials(env="qa"):
#     with open("C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\project_config\\cred_config.yml", "r") as file:
#         credentials = yaml.safe_load(file)
#     return credentials[env]
#
# # JDBC and ADLS credentials
# creds_sqlserver = load_credentials()['sqlserver']
# jdbc_url = creds_sqlserver['url']
# user = creds_sqlserver['user']
# password = creds_sqlserver['password']
# driver = creds_sqlserver['driver']
#
# jdbc_properties = {
#     "user": user,
#     "password": password,
#     "driver": driver
# }
#
# # Initialize Spark session with the required JARs
# spark = SparkSession.builder \
#     .appName("PySparkToAzureSQL") \
#     .config("spark.jars", "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\mssql-jdbc-12.2.0.jre8.jar") \
#     .getOrCreate()
#
# # Example DataFrame (replace with your actual data)
# final_df = spark.createDataFrame([("John", "john@example.com", 25)], ["name", "email", "age"])
#
# # Overwrite data to Azure SQL Server
# try:
#     final_df.write.jdbc(
#         url=jdbc_url,
#         table='customers_silver_scd2_expected',
#         mode='overwrite',  # This will overwrite the existing data
#         properties=jdbc_properties
#     )
#     print("Data successfully written to Azure SQL.")
# except Exception as e:
#     print(f"Error while writing to Azure SQL: {str(e)}")
#
# spark.stop()
