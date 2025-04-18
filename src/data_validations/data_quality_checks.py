from pyspark.sql.functions import col, regexp_extract, udf
import datetime
from pyspark.sql.types import BooleanType
from datetime import datetime
from src.utility.report_lib import write_output

def name_check(target, column):
    pattern = "^[a-zA-Z]"  # Matches alphabetic characters only
    # Add a new column 'is_valid' indicating if the name contains only alphabetic characters
    df = target.withColumn("is_valid", regexp_extract(col(column), pattern, 0) != "")

    # Show the DataFrame to inspect the result
    df.show()

    # Count total rows in target DataFrame
    target_count = target.count()

    # Filter rows where 'is_valid' is False
    failed = df.filter(col("is_valid") == False)
    failed.show()

    # Count rows with validation failure
    failed_count = failed.count()

    if failed_count > 0:
        failed_preview = [row.asDict() for row in failed.limit(5).collect()]
        status = 'FAIL'
        write_output(
            "Name Check",
            status,
            f"Invalid names in column '{column}': {failed_count}, Sample: {failed_preview}"
        )
    else:
        status = 'PASS'
        write_output("Name Check", status, f"All values in column '{column}' are valid names.")

    return status


def check_range(target, column, min_val, max_val):
    # Find rows where the value is outside the range
    invalid_rows = target.filter((col(column) < min_val) | (col(column) > max_val))

    # Show the rows that are out of range
    invalid_rows.show()

    invalid_count = invalid_rows.count()

    if invalid_count > 0:
        failed_preview = [row.asDict() for row in invalid_rows.limit(5).collect()]
        status = False
        write_output(
            "Check Range",
            "FAIL",
            f"Column '{column}' has {invalid_count},Sample Failed Records: {failed_preview}"
        )
    else:
        status = True
        write_output("Check Range", "PASS",
                     f"All values in column '{column}' are within the range [{min_val}, {max_val}].")

    return status




def date_check(target, column):
    def is_valid_date_format(date_str: str) -> bool:
        try:
            date_str = str(date_str)
            # Try to parse the string in the format 'dd-mm-yyyy'
            datetime.strptime(date_str, "%d-%m-%Y")
            return True
        except ValueError:
            return False

    print(f"\n Sample values from column '{column}':")
    target.select(column).show(10, truncate=False)

    # Register the UDF to check the date format
    date_format_udf = udf(is_valid_date_format, BooleanType())

    # Apply the UDF and create a new column 'is_valid_format'
    df_with_validation = target.withColumn("is_valid_format", date_format_udf(col(column)))

    # Filter rows where the date format is invalid
    invalid_dates = df_with_validation.filter(col("is_valid_format") == False)

    # Show rows with invalid date format
    invalid_dates.show()

    # Count rows with invalid date format
    failed = invalid_dates.count()

    if failed > 0:
        failed_preview = [row.asDict() for row in invalid_dates.limit(5).collect()]
        status = 'FAIL'
        write_output(
            "Date Check",
            status,
            f"Invalid dates in column '{column}': {failed}, Sample: {failed_preview}"
        )
    else:
        status = 'PASS'
        write_output("Date Check", status, f"All values in column '{column}' are valid dates in format 'dd-mm-yyyy'.")

    return status

