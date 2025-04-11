from src.utility.report_lib import write_output
from pyspark.sql import functions as F


def uniqueness_check(df, unique_cols):
    """Validate that specified columns have unique values."""
    duplicate_counts = {}  # {'identifier':0 , 'surname':0,'last_name':3}
    failed_records = {}  # {'identifier':[], 'surname':[], 'last_name':[]}

    for column in unique_cols:
        # Find rows with duplicate values
        duplicates = df.groupBy(column).count().filter("count > 1")

        # Count duplicates
        count_duplicates = duplicates.count()
        print("count_duplicates", column, count_duplicates)
        duplicate_counts[column] = count_duplicates

        if count_duplicates > 0:
            # Collect sample records where the value is duplicated
            failed_records[column] = duplicates.limit(5).collect()  # Get first 5 duplicates for each column

    print("duplicate_counts", duplicate_counts)

    # Determine overall status
    status = "PASS" if all(count == 0 for count in duplicate_counts.values()) else "FAIL"

    # Prepare output message
    if status == "FAIL":
        # Prepare preview of failed records for reporting
        failed_preview = {col: [row.asDict() for row in failed_records.get(col, [])] for col in failed_records}
        message = f"Duplicate counts per column: {duplicate_counts}, Sample Failed Records: {failed_preview}"
    else:
        message = f"Duplicate counts per column: {duplicate_counts}, No uniqueness violations."

    write_output(
        "Uniqueness Check",
        status,
        message
    )
    return status

# from src.utility.report_lib import write_output
# #from pyspark.sql import SparkSession
# #from src.data_validations.duplicate_validation import duplicate_check
#
# # spark = SparkSession.builder.master("local[1]").appName('test').getOrCreate()
#
# def uniqueness_check(df, unique_cols):
#     """Validate that specified columns have unique values."""
#     duplicate_counts = {}  #  {'identifier':0 , 'surname':0,'last_name':3}
#     for column in unique_cols:
#         count_duplicates = df.groupBy(column).count().filter("count > 1").count()
#         print("count_duplicates", column, count_duplicates)
#         duplicate_counts[column] = count_duplicates
#
#     print("duplicate_counts", duplicate_counts)
#
#     status = "PASS" if all(count == 0 for count in duplicate_counts.values()) else "FAIL"
#     write_output(
#         "Uniqueness Check",
#         status,
#         f"Duplicate counts per column: {duplicate_counts}"
#     )
#     return status
#
# # df = spark.read.csv('C:\\Users\\souls\\PycharmProjects\\MyProj\\taf_dec\\input_files\\Contact_info_t.csv', header=True, inferSchema=True)
# # print(uniqueness_check(df=df, unique_cols=['Identifier','Surname','given_name']))
# # print(duplicate_check(df=df, key_col=['Identifier','Surname','given_name']))