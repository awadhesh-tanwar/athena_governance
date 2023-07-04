import pyathena
import awswrangler

from app.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME

conn = pyathena.connect(region_name=AWS_REGION_NAME,
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        s3_staging_dir='s3://ds-lake-sbox-test-s3/awd/staging/')


def write_db_permisssions_table_to_s3(df):
    awswrangler.s3.to_parquet(df=df, path="s3://ds-lake-sbox-test-s3/awd/default/db_permissions/db_permissions.parquet",
                              index=False)


def write_table_permissions_table_to_s3(df):
    awswrangler.s3.to_parquet(df=df,
                              path="s3://ds-lake-sbox-test-s3/awd/default/table_permissions/table_permissions.parquet",
                              index=False)


def write_lftags_db_permissions_table_to_s3(df):
    awswrangler.s3.to_parquet(df=df,
                              path="s3://ds-lake-sbox-test-s3/awd/default/lftags_db_permissions/lftags_db_permissions"
                                   ".parquet",
                              index=False)


def dump_db_permissions_table_into_athena(df):
    query_create_db_permissions_table = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS `default.db_permissions`(
        `db_name`string,
        `db_arn` boolean,
        `principal` string,
        `user_name` string,
        `p_ALL` boolean,
        `p_ALTER` boolean,
        `p_CREATE_TABLE` boolean,
        `p_DESCRIBE` boolean,
        `p_DROP` boolean
    )ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      's3://ds-lake-sbox-test-s3/awd/default/db_permissions/'
    TBLPROPERTIES (
      'classification'='parquet')
    '''

    try:
        write_db_permisssions_table_to_s3(df)
        cursor = pyathena.connect(s3_staging_dir="s3://ds-lake-sbox-test-s3/awd/staging/", region_name=AWS_REGION_NAME,
                                  schema_name='default').cursor()
        cursor.execute(query_create_db_permissions_table)
        cursor.execute('SELECT * FROM default.db_permissions limit 10;')
        a = cursor.fetchall()
        print(a)
    except Exception as e:
        print('error:', e)
        return False

    return True


def dump_table_permissions_table_into_athena(df):
    query_create_table_permissions_table = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS `default.table_permissions`(
        `db_name` string,
        `table_name` string,
        `table_arn` boolean,
        `principal` string,
        `user_name` string,
        `p_ALL` boolean,
        `p_ALTER` boolean,
        `p_DELETE` boolean,
        `p_DESCRIBE` boolean,
        `p_DROP` boolean,
        `p_INSERT` boolean,
        `p_SELECT` boolean
    ) ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://ds-lake-sbox-test-s3/awd/default/table_permissions/'
    TBLPROPERTIES (
        'classification'='parquet')
    '''

    try:
        write_table_permissions_table_to_s3(df)
        cursor = pyathena.connect(s3_staging_dir="s3://ds-lake-sbox-test-s3/awd/staging/table_permissions/",
                                  region_name=AWS_REGION_NAME, schema_name='default').cursor()
        cursor.execute(query_create_table_permissions_table)
    except Exception as e:
        print('error:', e)
        return False

    return True


def dump_lftags_db_table_into_athena(df):
    query_create_lftags_db_permissions_table = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS `default.lftags_db_permissions`(
        `lftag_key` boolean, 
        `lftag_value` boolean,
        `db_name`string,
        `db_arn` boolean,
        `principal` string,
        `user_name` string,
        `p_ALL` boolean,
        `p_ALTER` boolean,
        `p_CREATE_TABLE` boolean,
        `p_DESCRIBE` boolean,
        `p_DROP` boolean
    ) ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        's3://ds-lake-sbox-test-s3/awd/default/lftags_db_permissions/'
    TBLPROPERTIES (
        'classification'='parquet')
    '''

    try:
        write_lftags_db_permissions_table_to_s3(df)
        cursor = pyathena.connect(s3_staging_dir="s3://ds-lake-sbox-test-s3/awd/staging/lftags_db_permissions/",
                                  region_name=AWS_REGION_NAME, schema_name='default').cursor()
        cursor.execute(query_create_lftags_db_permissions_table)
    except Exception as e:
        print('error:', e)
        return False

    return True
