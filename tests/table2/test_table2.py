# from src.data_validations.count_check import count_val
# def test_count_check(read_data,read_config):
#     source, target = read_data
#     read_config = read_config
#     key_columns = read_config['validations']['count_check']['key_columns']
#     status = count_val(source=source, target=target,key_columns=key_columns)
#     assert status == 'PASS'
from src.data_validations.count_check import count_val
from src.data_validations.duplicate_validation import duplicate_check
from src.data_validations.uniqueness_check import uniqueness_check
from src.data_validations.null_validation import null_value_check
from src.data_validations.data_compare import data_compare
from src.data_validations.schema_validation import schema_check
from src.data_validations.data_quality_checks import name_check, check_range, date_check

def test_count_check(read_data,read_config):
    source, target = read_data
    #read_config = read_config
    key_columns = read_config['validations']['count_check']['key_columns']
    status = count_val(source=source, target=target,key_columns=key_columns)
    assert status == 'PASS'

def test_duplicate_check(read_data,read_config):
    source, target = read_data
    #read_config = read_config
    key_columns = read_config['validations']['duplicate_check']['key_columns']
    status = duplicate_check( df=target,key_col=key_columns)
    assert status == 'PASS'

def test_uniqueness_check(read_data,read_config):
    source, target = read_data
    #read_config = read_config
    unique_cols = read_config['validations']['uniqueness_check']['unique_columns']
    status = uniqueness_check( df=target,unique_cols=unique_cols)
    assert status == 'PASS'

def test_null_check(read_data,read_config):
    source, target = read_data
    #read_config = read_config
    null_cols = read_config['validations']['null_check']['null_columns']
    status = null_value_check( df=target,null_cols=null_cols)
    assert status == 'PASS'

def test_data_compare_check(read_data,read_config):
    source, target = read_data
    #read_config = read_config
    key_columns = read_config['validations']['data_compare_check']['key_column']
    num_records = read_config['validations']['data_compare_check']['num_records']
    status = data_compare(source=source, target=target, key_column=key_columns, num_records=num_records)
    assert status == 'PASS'

def test_schema_check(read_data,read_config,spark_session):
    source, target = read_data
    read_config = read_config
    status = schema_check(source=source,target=target,spark=spark_session)
    assert status == 'PASS'

def test_name_check(read_data,read_config):
    source, target = read_data
    column = read_config['data_quality_checks']['name_check']['column']
    status = name_check(target,column)
    assert status == 'PASS'


def test_check_range(read_data, read_config):
    source, target = read_data
    check_range_config = read_config['data_quality_checks']['check_range']
    column = check_range_config['column']
    min_val = check_range_config['min_val']
    max_val = check_range_config['max_val']
    status = check_range(target, column, min_val, max_val)
    assert status is True
 #we are using status is True because the return statement of the fucntion check_range is boolean either 0 or 1 and not a string.

def test_date_check(read_data,read_config):
    source,target = read_data
    column = read_config['data_quality_checks']['date_check']['column']
    status = date_check(target,column)
    assert status == 'PASS'





