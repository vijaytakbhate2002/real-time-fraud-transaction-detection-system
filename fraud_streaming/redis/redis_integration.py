import json
from datetime import datetime, timezone

import redis

from fraud_streaming.redis.config import (
    FEATURE_STORE_INDEX_KEY,
    FEATURE_STORE_KEY_PREFIX,
    WINDOW_TIME,
)

class RedisIntegration:
    def __init__(self, host="localhost", port=6379, db=0, password=None):
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
        )

    def write_transactions_to_redis(self, batch_df):
        """Store transaction records in Redis for the configured sliding window."""
        ingested_at = datetime.now(timezone.utc)
        ingested_at_ms = int(ingested_at.timestamp() * 1000)
        window_ms = WINDOW_TIME * 60 * 1000
        cutoff_ms = ingested_at_ms - window_ms
        transactions = [json.loads(row) for row in batch_df.toJSON().collect()]

        if not transactions:
            return

        stale_keys = self.redis_client.zrangebyscore(
            FEATURE_STORE_INDEX_KEY, "-inf", cutoff_ms
        )
        pipeline = self.redis_client.pipeline()
        if stale_keys:
            pipeline.delete(*stale_keys)
        pipeline.zremrangebyscore(FEATURE_STORE_INDEX_KEY, "-inf", cutoff_ms)

        for transaction in transactions:
            redis_key = f"{FEATURE_STORE_KEY_PREFIX}{transaction['trans_num']}"
            transaction["ingested_at"] = ingested_at.isoformat(timespec="milliseconds")
            pipeline.set(
                redis_key,
                json.dumps(transaction, separators=(",", ":")),
                px=window_ms,
            )
            pipeline.zadd(FEATURE_STORE_INDEX_KEY, {redis_key: ingested_at_ms})

        pipeline.expire(FEATURE_STORE_INDEX_KEY, WINDOW_TIME * 60)
        pipeline.execute()

    def close(self):
        self.redis_client.close()
