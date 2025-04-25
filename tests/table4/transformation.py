from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import yaml
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_credentials(env="qa"):
    """Load credentials from centralized YAML file."""
    base_dir = os.environ.get("PROJECT_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cred_path = os.path.join(base_dir, "project_config", "cred_config.yml")

    logger.info(f"Loading credentials from: {cred_path}")
    with open(cred_path, "r") as file:
        credentials = yaml.safe_load(file)
    return credentials[env]

def get_jar_path():
    """Get path to required JAR files from env or default fallback."""
    return os.environ.get("POSTGRES_JAR", os.path.join(os.getcwd(), "jars", "postgresql-42.2.5.jar"))

def get_input_file_path():
    """Get CSV input path from env or default fallback."""
    return os.environ.get("CUSTOMERS_CSV", os.path.join(os.getcwd(), "input_files", "customers.csv"))

def main():
    jar_path = get_jar_path()
    logger.info(f"Using PostgreSQL JAR: {jar_path}")

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest_framework_transformation") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()

    env = os.environ.get("ENV", "qa")
    creds = load_credentials(env)["postgres"]

    logger.info("Reading from PostgreSQL source table...")
    source1 = spark.read.format("jdbc") \
        .option("url", creds["url"]) \
        .option("user", creds["user"]) \
        .option("password", creds["password"]) \
        .option("query", "SELECT id, first_name FROM employees") \
        .option("driver", creds["driver"]) \
        .load()

    source1 = source1.withColumn("source_id", lit("postgres"))

    customers_csv_path = get_input_file_path()
    logger.info(f"Reading customer CSV from: {customers_csv_path}")
    source2 = spark.read.csv(customers_csv_path, header=True, inferSchema=True) \
        .select("id", "first_name") \
        .withColumn("source_id", lit("file"))

    logger.info("Union of PostgreSQL and CSV data...")
    combined = source1.unionByName(source2)

    logger.info("Writing combined data to employees_expected table...")
    combined.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", creds["url"]) \
        .option("user", creds["user"]) \
        .option("password", creds["password"]) \
        .option("dbtable", "employees_expected") \
        .option("driver", creds["driver"]) \
        .save()

    logger.info("Data transformation complete.")
    spark.stop()

if __name__ == "__main__":
    main()








#this commeneted code works perfectly fine in local not for github actions#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import lit
# import yaml
#
# def load_credentials(env="qa"):
#     """Load credentials from the centralized YAML file."""
#
#     credentials_path = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\project_config\\cred_config.yml'
#
#     with open(credentials_path, "r") as file:
#         credentials = yaml.safe_load(file)
#         print(credentials[env])
#     return credentials[env]
#
#
# postgres_jar = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\postgresql-42.2.5.jar'
#
# jar_path = postgres_jar
# spark = SparkSession.builder.master("local[1]") \
#         .appName("pytest_framework") \
#         .config("spark.jars", jar_path) \
#         .config("spark.driver.extraClassPath", jar_path) \
#         .config("spark.executor.extraClassPath", jar_path) \
#         .getOrCreate()
#
#
# creds = load_credentials()
# creds = creds['postgres']
#
# source1 = spark.read.format("jdbc"). \
#     option("url", creds['url']). \
#     option("user", creds['user']). \
#     option("password", creds['password']). \
#     option("query", "select id, first_name from employees"). \
#     option("driver", creds['driver']).load()
#
#
#
# source1 = source1.withColumn('source_id', lit('postgres'))
#
#
# source2 = spark.read.csv("C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\input_files\\customers.csv", header=True, inferSchema=True)
#
# source2 = source2.select("id", 'first_name').withColumn('source_id', lit('file'))
#
# source = source1.unionAll(source2)
#
# (source.write.mode("overwrite")
#     .format("jdbc")
#     .option("url", creds['url'])
#     .option("user", creds['user'])
#     .option("password", creds['password'])
#     .option("dbtable", "employees_expected")
#     .option("driver", creds['driver'])
#     .save())