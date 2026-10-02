import redis

class RedisIntegration:
    def __init__(self, host="localhost", port=6379, db=0, password=None):
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
        )

    def write_metrics_to_redis(self, batch_df, batch_id):
        """Write simple transaction and fraud totals for one Spark batch."""
        total_records = batch_df.count()
        fraud_count = batch_df.filter("is_fraud = 1").count()

        self.redis_client.hset(
            "fraud:metrics:latest",
            mapping={
                "batch_id": batch_id,
                "total_records": total_records,
                "fraud_count": fraud_count,
            },
        )

    def close(self):
        self.redis_client.close()
