import sys
import os
from pyspark.sql import SparkSession
import pytest
import yaml
import json
from pyspark.sql.types import StructType
from src.utility.general_utility import flatten
import subprocess
import platform

def get_jar_path():
    base_jar_dir = os.environ.get('SPARK_JAR_DIR', os.path.join(os.getcwd(), 'jars'))

    jars = {
        "snowflake": "snowflake-jdbc-3.22.0.jar",
        "postgres": "postgresql-42.2.5.jar",
        "azure_storage": "azure-storage-8.6.6.jar",
        "hadoop_azure": "hadoop-azure-3.3.1.jar",
        "sql_server": "mssql-jdbc-12.2.0.jre8.jar"
    }

    jar_paths = [os.path.join(base_jar_dir, jar) for jar in jars.values()]
    return ",".join(jar_paths)

@pytest.fixture(scope='session')
def spark_session(request):
    jar_path = get_jar_path()
    spark = SparkSession.builder.master("local[*]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()

    env = os.environ.get('ENV', 'qa')
    cred = load_credentials(env)['adls']
    adls_account_name = cred['adls_account_name']
    key = cred['key']
    spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "SharedKey")
    spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)

    return spark

@pytest.fixture(scope='module')
def read_config(request):
    dir_path = os.path.dirname(request.node.fspath)
    config_path = os.path.join(dir_path, 'config.yml')
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    return config_data

def read_schema(dir_path):
    schema_path = os.path.join(dir_path, 'schema.json')
    with open(schema_path, 'r') as f:
        return StructType.fromJson(json.load(f))

def read_query(dir_path):
    sql_path = os.path.join(dir_path, 'transformation.sql')
    with open(sql_path, 'r') as f:
        return f.read()

def read_file(config_data, spark, dir_path):
    df = None
    if config_data['type'] == 'csv':
        schema = read_schema(dir_path) if config_data.get('schema') == 'Y' else None
        df = spark.read.schema(schema).csv(config_data['path'], header=config_data['options']['header']) if schema \
            else spark.read.csv(config_data['path'], header=config_data['options']['header'], inferSchema=True)
    elif config_data['type'] == 'json':
        df = spark.read.json(config_data['path'], multiLine=config_data['options']['multiline'])
        df = flatten(df)
    elif config_data['type'] == 'parquet':
        df = spark.read.parquet(config_data['path'])
    elif config_data['type'] == 'avro':
        df = spark.read.format('avro').load(config_data['path'])
    return df

def read_db(config_data, spark, dir_path):
    creds = load_credentials()[config_data['cred_lookup']]
    if config_data['transformation'][0].lower() == 'y' and config_data['transformation'][1].lower() == 'sql':
        sql_query = read_query(dir_path)
        df = spark.read.format("jdbc") \
            .option("url", creds['url']) \
            .option("user", creds['user']) \
            .option("password", creds['password']) \
            .option("query", sql_query) \
            .option("driver", creds['driver']) \
            .load()
    else:
        df = spark.read.format("jdbc") \
            .option("url", creds['url']) \
            .option("user", creds['user']) \
            .option("password", creds['password']) \
            .option("dbtable", config_data['table']) \
            .option("driver", creds['driver']) \
            .load()
    return df

@pytest.fixture(scope='module')
def read_data(read_config, spark_session, request):
    spark = spark_session
    config_data = read_config
    dir_path = os.path.dirname(request.node.fspath)
    source_config = config_data['source']
    target_config = config_data['target']

    if source_config['type'] == 'database' and source_config['transformation'][1].lower() == 'python':
        transformation_path = os.path.join(dir_path, 'transformation.py')
        subprocess.run([sys.executable, transformation_path], check=True)

    source_df = read_db(source_config, spark, dir_path) if source_config['type'] == 'database' \
        else read_file(source_config, spark, dir_path)
    target_df = read_db(target_config, spark, dir_path) if target_config['type'] == 'database' \
        else read_file(target_config, spark, dir_path)

    return source_df.drop(*source_config['exclude_cols']), target_df.drop(*target_config['exclude_cols'])

def load_credentials(env="qa"):
    taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cred_path = os.path.join(taf_path, 'project_config', 'cred_config.yml')
    with open(cred_path, 'r') as f:
        credentials = yaml.safe_load(f)
    return credentials[env]



# #the code from here till last commented part works perfectly fine in local system but in github workdflow it fails.
# import sys
# import os
# from pyspark.sql import SparkSession
# import pytest
# import yaml
# import json
# from pyspark.sql.types import StructType, ArrayType
# from src.utility.general_utility import flatten
# #from utility.general_utility import flatten
# import subprocess
#
# # Modify sys.path to include the 'src' directory-
# #sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
#
# @pytest.fixture(scope='session')
# def spark_session(request):
#     #dir_path = request.node.fspath.dirname
#
#     snow_jar = "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\snowflake-jdbc-3.22.0.jar"
#     postgres_jar = "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\postgresql-42.2.5.jar"
#     azure_storage = "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\azure-storage-8.6.6.jar"
#     hadoop_azure = "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\hadoop-azure-3.3.1.jar"
#     sql_server = "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\mssql-jdbc-12.2.0.jre8.jar"
#     jar_path = snow_jar + ',' + postgres_jar + ',' + azure_storage + ',' + hadoop_azure + ',' + sql_server
#     spark = SparkSession.builder.master("local[1]") \
#         .appName("pytest_framework") \
#         .config("spark.jars", jar_path) \
#         .config("spark.driver.extraClassPath", jar_path) \
#         .config("spark.executor.extraClassPath", jar_path) \
#         .getOrCreate()
#
#     cred = load_credentials('qa')['adls']
#     adls_account_name = cred['adls_account_name']
#     key = cred['key']
#     spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "SharedKey")
#     spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)
#
#     return spark
#     #yield spark
#     #spark.stop()
#
# @pytest.fixture(scope='module')
# def read_config(request):
#     print("request.node.fspath.dirname", request.node.fspath.dirname)
#     dir_path = request.node.fspath.dirname
#     config_path = dir_path + '\\config.yml'
#     with open(config_path, 'r') as f:
#         config_data = yaml.safe_load(f)
#     return config_data
#
#
# def read_schema(dir_path):
#     schema_path = dir_path + '\\schema.json'
#     with open(schema_path, 'r') as schema_file:
#         schema = StructType.fromJson(json.load(schema_file))
#     return schema
#
# def read_query(dir_path):
#     sql_query_path = dir_path + '\\transformation.sql'
#     with open(sql_query_path, "r") as file:
#         sql_query = file.read()
#     return sql_query
#
#
# def read_file(config_data,spark, dir_path):
#     df = None
#     if config_data['type'] == 'csv':
#         if config_data['schema'] == 'Y':
#             schema = read_schema(dir_path)
#             df = spark.read.schema(schema).csv(config_data['path'], header=config_data['options']['header'])
#         else:
#             df = spark.read.csv(config_data['path'], header= config_data['options']['header'],inferSchema=True)
#     elif config_data['type'] == 'json':
#         df = spark.read.json(config_data['path'], multiLine=config_data['options']['multiline'] )
#         df = flatten(df)
#     elif config_data['type'] == 'parquet':
#         df = spark.read.parquet(config_data['path'])
#     elif config_data['type'] == 'avro':
#         df = spark.read.format('avro').load(config_data['path'])
#     elif config_data['type'] == 'txt':
#         pass
#     return df
#
# def read_db(config_data,spark,dir_path):
#     #df = None
#     creds = load_credentials()
#     cred_lookup = config_data['cred_lookup']
#     creds = creds[cred_lookup]
#     print("creds", creds)
#     if config_data['transformation'][0].lower() == 'y' and config_data['transformation'][1].lower() == 'sql':
#         sql_query= read_query(dir_path)
#         print("sql_query", sql_query)
#         df = spark.read.format("jdbc"). \
#             option("url", creds['url']). \
#             option("user", creds['user']). \
#             option("password", creds['password']). \
#             option("query", sql_query). \
#             option("driver", creds['driver']).load()
#
#     else:
#         df = spark.read.format("jdbc"). \
#             option("url", creds['url']). \
#             option("user", creds['user']). \
#             option("password", creds['password']). \
#             option("dbtable", config_data['table']). \
#             option("driver", creds['driver']).load()
#     return df
#
# @pytest.fixture(scope='module')
# def read_data(read_config,spark_session,request ):
#     spark = spark_session
#     config_data = read_config
#     source_config = config_data['source']
#     target_config = config_data['target']
#     dir_path = request.node.fspath.dirname
#     if source_config['type'] == 'database':
#         if source_config['transformation'][1].lower() == 'python' and source_config['transformation'][0].lower() == 'y':
#             python_file_path = dir_path + '/transformation.py'
#             print("python file name", python_file_path)
#             #subprocess.run(["python", python_file_path])
#             subprocess.run([sys.executable,python_file_path])
#         source = read_db(config_data=source_config,spark=spark,dir_path=dir_path)
#     else:
#         source = read_file(config_data = source_config,spark=spark, dir_path=dir_path)
#     if target_config['type'] == 'database':
#         target = read_db(config_data=target_config,spark=spark,dir_path=dir_path)
#     else:
#         target = read_file(config_data =target_config,spark=spark,dir_path=dir_path)
#
#     print("target_exclude", target_config['exclude_cols'])
#
#     return source.drop(*source_config['exclude_cols']), target.drop(*target_config['exclude_cols'])#we are using *source_config here to exclude all columns present in
# #exclude_cols
#
#
# def load_credentials(env="qa"):
#     """Load credentials from the centralized YAML file."""
#     taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#     credentials_path = taf_path+'\\project_config\\cred_config.yml'
#
#     with open(credentials_path, "r") as file:
#         credentials = yaml.safe_load(file)
#         print(credentials[env])
#     return credentials[env]


















