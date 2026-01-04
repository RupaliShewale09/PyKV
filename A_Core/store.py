from .lru import LRUCache

NUM_SHARDS = 16

class CoreStore:
    """ Clean Interface for API
    Hides internal cache logic, exposes simple operations """
    def __init__(self, capacity=50):
        per_shard_capacity = max(1, capacity // NUM_SHARDS)
        self.shards = [
            LRUCache(per_shard_capacity)
            for _ in range(NUM_SHARDS)
        ]

    def _get_shard(self, key):
        return self.shards[hash(key) % NUM_SHARDS]

    def put(self, key, value, ttl=None):          # insert key
        return self._get_shard(key).set(key, value, ttl)

    def get(self, key):                 # fetch key
        return self._get_shard(key).get(key)

    def update(self, key, value, ttl=None):       # modify values
        return self._get_shard(key).update(key, value, ttl)

    def delete(self, key):              # remove key
        return self._get_shard(key).delete(key)

    def list_keys(self, prefix=None):       #list all keys  LRU -> MRU
        keys = []
        for shard in self.shards:
            shard_keys = shard.list_keys()
            if prefix:
                shard_keys = [k for k in shard_keys if k.startswith(prefix)]
            keys.extend(shard_keys)
        return keys

    def dump_all(self):
        data = {}
        for shard in self.shards:
            data.update(shard.dump())
        return data

    # -------- Stats aggregation --------
    @property
    def cache(self):
        return self  # compatibility with /stats

    @property
    def capacity(self):
        return sum(shard.capacity for shard in self.shards)

    @property
    def hits(self):
        return sum(getattr(shard, "hits", 0) for shard in self.shards)

    @property
    def misses(self):
        return sum(getattr(shard, "misses", 0) for shard in self.shards)

    @property
    def evictions(self):
        return sum(getattr(shard, "evictions", 0) for shard in self.shards)

