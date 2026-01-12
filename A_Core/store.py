from .lru import LRUCache


class CoreStore:
    """ Clean Interface for API
    Hides internal cache logic, exposes simple operations """
    def __init__(self, capacity=100):
        self.NUM_SHARDS = 1 if capacity < 32 else 16
        base = capacity // self.NUM_SHARDS
        extra = capacity % self.NUM_SHARDS

        self.shards = []
        for i in range(self.NUM_SHARDS):
            shard_capacity = base + (1 if i < extra else 0)
            self.shards.append(LRUCache(shard_capacity))

    def _get_shard(self, key):
        return self.shards[hash(key) % self.NUM_SHARDS]

    def put(self, key, value, ttl=None):          # insert key
        return self._get_shard(key).set(key, value, ttl)

    def get(self, key):                 # fetch key
        return self._get_shard(key).get(key)

    def update(self, key, value, ttl=None):       # modify values
        return self._get_shard(key).update(key, value, ttl)

    def delete(self, key):              # remove key
        return self._get_shard(key).delete(key)

    def list_keys(self, prefix=None):       #list all keys  LRU -> MRU
        all_data = self.dump_all() 
        if prefix:
            return [k for k in all_data.keys() if k.startswith(prefix)]
        return list(all_data.keys())
    
    def dump_all(self):
        all_data = {}
        for shard in self.shards:
            all_data.update(shard.dump())

        sorted_items = sorted(
            all_data.items(), 
            key=lambda x: x[1].get('last_accessed',0)
        )
        
        return {k: v for k, v in sorted_items}
    
    def purge_expired(self):
        for shard in self.shards:
            with shard.lock:
                expired_keys = [
                    k for k, node in shard.map.items()
                    if shard._is_expired(node)
                ]
                for k in expired_keys:
                    shard.dll.remove_node(shard.map[k])
                    del shard.map[k]


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

