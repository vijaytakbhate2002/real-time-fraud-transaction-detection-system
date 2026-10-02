import redis

class RedisIntegration:
    def __init__(self, host, port):
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            decode_responses=True
        )
