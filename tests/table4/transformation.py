from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import yaml
import os
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_credentials(env="qa"):
    """Load credentials from centralized YAML file."""
    taf_path = Path(__file__).resolve().parent.parent.parent
    cred_path = taf_path / 'project_config' / 'cred_config.yml'

    logger.info(f"Loading credentials from: {cred_path}")
    try:
        with open(cred_path, "r") as file:
            credentials = yaml.safe_load(file)
        return credentials[env]
    except FileNotFoundError:
        logger.error(f"Credentials file not found at {cred_path}")
        raise  # Re-raise the exception to signal failure to the caller
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML in {cred_path}: {e}")
        raise  # Re-raise
    except KeyError as e:
        logger.error(f"Environment '{env}' not found in credentials file.")
        raise

def get_jar_path():
    """Get path to required JAR files."""
    # changed this to be relative
    project_root = Path(__file__).resolve().parent.parent.parent
    jar_path = project_root / "jars" / "postgresql-42.2.5.jar"
    if not jar_path.exists():
        logger.error(f"PostgreSQL JAR not found at {jar_path}")
        raise FileNotFoundError(f"PostgreSQL JAR not found at {jar_path}")
    return str(jar_path)



def get_spark_session():
    """Gets or creates a SparkSession with the PostgreSQL JAR."""
    jar_path = get_jar_path()
    logger.info(f"Using PostgreSQL JAR: {jar_path}")
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest_framework_transformation") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()
    return spark

def main():
    """Main function to run the data transformation."""
    spark = get_spark_session()
    try:
        env = os.environ.get("ENV", "qa")
        creds = load_credentials(env)["postgres"]  # Get postgres credentials
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        spark.stop()
        raise  # Re-raise to cause non-zero exit

    logger.info("Reading from PostgreSQL source table...")
    try:
        source1 = spark.read.format("jdbc") \
            .option("url", creds["url"]) \
            .option("user", creds["user"]) \
            .option("password", creds["password"]) \
            .option("query", "SELECT id, first_name FROM employees") \
            .option("driver", creds["driver"]) \
            .load()
    except Exception as e:
        logger.error(f"Failed to read from PostgreSQL: {e}")
        spark.stop()
        raise

    source1 = source1.withColumn("source_id", lit("postgres"))

    csv_path = Path(__file__).resolve().parent.parent.parent / "input_files" / "customers.csv"
    logger.info(f"Reading customer CSV from: {csv_path}")
    try:
        source2 = spark.read.csv(str(csv_path), header=True, inferSchema=True) \
            .select("id", "first_name") \
            .withColumn("source_id", lit("file"))
    except Exception as e:
        logger.error(f"Failed to read CSV file: {e}")
        spark.stop()
        raise

    logger.info("Union of PostgreSQL and CSV data...")
    combined = source1.unionByName(source2)

    logger.info("Writing combined data to employees_expected table...")
    try:
        combined.write.mode("overwrite") \
            .format("jdbc") \
            .option("url", creds["url"]) \
            .option("user", creds["user"]) \
            .option("password", creds["password"]) \
            .option("dbtable", "employees_expected") \
            .option("driver", creds["driver"]) \
            .save()
    except Exception as e:
        logger.error(f"Failed to write to PostgreSQL table: {e}")
        spark.stop()
        raise # important: re raise

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