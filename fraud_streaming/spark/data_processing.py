import sys
from pathlib import Path

import pyspark.sql.functions as F
from config import processed_data_path

try:
    from fraud_streaming.redis.config import host as redis_host, port as redis_port
    from fraud_streaming.redis.redis_integration import RedisIntegration
except ModuleNotFoundError:
    # Also support running the notebook with fraud_streaming/spark as cwd.
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from fraud_streaming.redis.config import host as redis_host, port as redis_port
    from fraud_streaming.redis.redis_integration import RedisIntegration


def write_to_parquet(transactions_df, batch_id):
    transactions_df.write \
        .mode("append") \
        .parquet(processed_data_path.as_posix())
    print(f"Processed and wrote batch {batch_id}")


def write_transactions_to_redis(batch_df):
    redis_store = RedisIntegration(host=redis_host, port=redis_port)
    try:
        redis_store.write_transactions_to_redis(batch_df)
    finally:
        redis_store.close()


def process_and_write_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    cleaned_transactions_df = (
        batch_df
        .withColumn("trans_date_trans_time", F.to_timestamp("trans_date_trans_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("dob", F.to_date("dob", "yyyy-MM-dd"))
        .withColumn("amt", F.col("amt").cast("double"))
        .withColumn("lat", F.col("lat").cast("double"))
        .withColumn("long", F.col("long").cast("double"))
        .withColumn("merch_lat", F.col("merch_lat").cast("double"))
        .withColumn("merch_long", F.col("merch_long").cast("double"))
        .withColumn("city_pop", F.col("city_pop").cast("long"))
        .withColumn("unix_time", F.col("unix_time").cast("long"))
        .withColumn("is_fraud", F.col("is_fraud").cast("int"))
        .withColumn("trans_num", F.trim("trans_num"))
        .withColumn("category", F.lower(F.trim("category")))
        .filter(
            F.col("trans_date_trans_time").isNotNull()
            & F.col("trans_num").isNotNull()
            & (F.length("trans_num") > 0)
            & F.col("amt").isNotNull()
            & (F.col("amt") >= 0)
            & F.col("cc_num").isNotNull()
            & F.col("unix_time").isNotNull()
            & F.col("is_fraud").isin(0, 1)
        )
        .dropDuplicates(["trans_num"])
    )

    transactions_df = cleaned_transactions_df \
        .withColumn("transaction_hour", F.hour("trans_date_trans_time")) \
        .withColumn("transaction_day_of_week", F.dayofweek("trans_date_trans_time")) \
        .withColumn("is_weekend", F.dayofweek("trans_date_trans_time").isin(1, 7).cast("int")) \
        .withColumn("amt_log1p", F.log1p("amt")) \
        .withColumn(
            "distance_km",
            F.round(
                6371.0 * 2 * F.asin(
                    F.sqrt(
                        F.least(
                            F.lit(1.0),
                            F.pow(F.sin(F.radians(F.col("merch_lat") - F.col("lat")) / 2), 2)
                            + F.cos(F.radians("lat")) * F.cos(F.radians("merch_lat"))
                            * F.pow(F.sin(F.radians(F.col("merch_long") - F.col("long")) / 2), 2),
                        )
                    )
                ),
                3,
            ),
        )

    write_to_parquet(transactions_df, batch_id)
    write_transactions_to_redis(transactions_df)
