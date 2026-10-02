from pyspark.sql.types import StructType, StructField, StringType

transaction_schema = StructType([
    StructField("trans_date_trans_time", StringType(), True),
    StructField("cc_num", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amt", StringType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True),
    StructField("city_pop", StringType(), True),
    StructField("job", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("trans_num", StringType(), True),
    StructField("unix_time", StringType(), True),
    StructField("merch_lat", StringType(), True),
    StructField("merch_long", StringType(), True),
    StructField("is_fraud", StringType(), True),
    StructField("merch_zipcode", StringType(), True)
])