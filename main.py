# import sys
# import subprocess
# # subprocess.run(["python","C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\test_dummy.py"])
#
#
# #subprocess.run(["python", "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\tests\\table4\\transformation.py"])
# subprocess.run([sys.executable,"C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\tests\\table4\\transformation.py"])

#from pyspark.sql import SparkSession
# from src.utility.general_utility import flatten
# # Initialize Spark session
#spark = (SparkSession.builder.master("local[2]")

#         .config("spark.jars", "C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\postgresql-42.2.5.jar")
#         .appName("pytest_framework")
#         .getOrCreate())
# print(spark.sparkContext.getConf().get("spark.jars"))
#
#
#
from pyspark.sql import SparkSession
azure_storage = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\azure-storage-8.6.6.jar'
hadoop_azure = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\hadoop-azure-3.3.1.jar'
sql_server = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\mssql-jdbc-12.2.0.jre8.jar'
postgres = 'C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\jars\\postgresql-42.2.5.jar'
jar_path =  azure_storage + ',' + hadoop_azure + ',' + sql_server +  ',' + postgres
spark = SparkSession.builder.master("local[*]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()
# #reading data from postgresql db in pyspark#
df = (spark.read.format('jdbc').
      option('url', "jdbc:postgresql://localhost:5432/postgres").
      option('user','postgres').
      option('password', 'Iphone9@').
      option('dbtable', 'customer').
      option('driver', "org.postgresql.Driver").load())
df.show()


# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('first_df_creation').getOrCreate()
# df4 = spark.read.json(r"C:\Users\souls\PycharmProjects\MyProj\taf_dec\input_files\Complex.json", multiLine=True)
# df4.show()
#
# from pyspark.sql.functions import explode, explode_outer, upper,lower, instr,lead,lag,substring,length,col
# from pyspark.sql.types import *
# def flatten(df):
#     # compute Complex Fields (Lists and Structs) in Schema
#     complex_fields = dict([(field.name, field.dataType)
#                            for field in df.schema.fields
#                            if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
#     while len(complex_fields) != 0:
#         col_name = list(complex_fields.keys())[0]
#         print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))
#
#         # if StructType then convert all sub element to columns.
#         # i.e. flatten structs
#         if type(complex_fields[col_name]) == StructType:
#             expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in
#                         [n.name for n in complex_fields[col_name]]]
#             df = df.select("*", *expanded).drop(col_name)
#
#         # if ArrayType then add the Array Elements as Rows using the explode function
#         # i.e. explode Arrays
#         elif type(complex_fields[col_name]) == ArrayType:
#             df = df.withColumn(col_name, explode_outer(col_name))
#
#         # recompute remaining Complex Fields in Schema
#         complex_fields = dict([(field.name, field.dataType)
#                                for field in df.schema.fields
#                                if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
#     return df
#
#
#
# from pyspark.sql.functions import first,explode,col
# df4_flatten = flatten(df4)
# df4_flatten.show()





