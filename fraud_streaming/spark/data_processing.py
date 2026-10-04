import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
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


# Now let's build a function to compute analytical insights from the processed data. So basically we will be reading data from redis and then computing some insights from it to show it on graphana dashboard.


def compute_window_analytics(spark, transactions):
    """Calculate dashboard metrics over the transactions currently in Redis."""
    from fraud_streaming.redis.config import WINDOW_TIME

    schema = StructType(
        [
            StructField("amt", DoubleType(), True),
            StructField("is_fraud", IntegerType(), True),
            StructField("category", StringType(), True),
        ]
    )
    rows = [
        (
            float(transaction["amt"]) if transaction.get("amt") is not None else None,
            int(transaction["is_fraud"])
            if transaction.get("is_fraud") is not None
            else None,
            (transaction.get("category") or "unknown").strip().lower() or "unknown",
        )
        for transaction in transactions
    ]
    transactions_df = spark.createDataFrame(rows, schema)

    aggregate = transactions_df.agg(
        F.count("*").alias("transaction_count"),
        F.sum("amt").alias("total_amount"),
        F.avg("amt").alias("average_amount"),
        F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count"),
    ).first()
    transaction_count = int(aggregate["transaction_count"] or 0)
    total_amount = float(aggregate["total_amount"] or 0.0)
    average_amount = float(aggregate["average_amount"] or 0.0)
    fraud_count = int(aggregate["fraud_count"] or 0)
    category_counts = {
        row["category"]: int(row["category_count"])
        for row in transactions_df.groupBy("category")
        .count()
        .withColumnRenamed("count", "category_count")
        .collect()
    }
    window_end = datetime.now(timezone.utc)

    return {
        "transaction_count": transaction_count,
        "total_amount": round(total_amount, 2),
        "average_amount": round(average_amount, 2),
        # "fraud_count": fraud_count,
        # "fraud_rate": (
        #     round(fraud_count / transaction_count, 6) if transaction_count else 0.0
        # ),
        "category_counts": category_counts,
        "window_start": (
            window_end - timedelta(minutes=WINDOW_TIME)
        ).isoformat(timespec="seconds"),
        "window_end": window_end.isoformat(timespec="seconds"),
        "updated_at": window_end.isoformat(timespec="seconds"),
    }
