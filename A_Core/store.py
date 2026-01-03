from .lru import LRUCache

class CoreStore:
    """ Clean Interface for API
    Hides internal cache logic, exposes simple operations """
    def __init__(self, capacity=50):
        self.cache = LRUCache(capacity)

    def put(self, key, value, ttl=None):          # insert key
        return self.cache.set(key, value, ttl)

    def get(self, key):                 # fetch key
        return self.cache.get(key)

    def update(self, key, value, ttl=None):       # modify values
        return self.cache.update(key, value, ttl)

    def delete(self, key):              # remove key
        return self.cache.delete(key)

    def list_keys(self, prefix=None):       #list all keys  LRU -> MRU
        keys = self.cache.list_keys()
        if prefix:
            return [k for k in keys if k.startswith(prefix)]
        return keys

    def dump_all(self):
        return self.cache.dump()

