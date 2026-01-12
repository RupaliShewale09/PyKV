from .dll import Node, DoublyLinkedList
from threading import Lock
import time

class LRUCache:
    def __init__(self, capacity=100):
        self.capacity = capacity
        self.map = {}           #Dictionary - O(1) lookup (i.e HashMap)
        self.dll = DoublyLinkedList()   # maintain usage order
        self.lock = Lock()      # thread safety             

        #Metrics
        self.evictions = 0
        self.hits = 0
        self.misses = 0 
        

    def _is_expired(self, node):
        return node.expiry is not None and node.expiry <= time.time()
    
    def get(self, key):     
        with self.lock:
            node = self.map.get(key)
            if not node:        # key not found -> None
                self.misses += 1
                return None
            
            if self._is_expired(node):
                self.dll.remove_node(node)
                del self.map[key]
                self.misses += 1
                return None
            
            self.hits +=1    
            node.last_accessed = time.time()        
            self.dll.move_to_tail(node)       # move to MRU
            return node.value

    def set(self, key, value, ttl=None):
        with self.lock:
            if key in self.map:        #If key exists → reject insert 
                node = self.map[key]
                if self._is_expired(node):
                    # Key expired → remove and allow new insert
                    self.dll.remove_node(node)
                    del self.map[key]
                else:
                    return False 

            expiry = time.time() + ttl if ttl is not None else None
            node = Node(key, value, expiry)         
            self.dll.add_to_tail(node)      # else add to MRU
            self.map[key] = node

            if len(self.map) > self.capacity:          # if capacity exceed
                lru = self.dll.remove_head()        # remove LRU
                if lru:
                    del self.map[lru.key]
                    self.evictions += 1
            return True

    def update(self, key, value, ttl=None):
        with self.lock:
            if key not in self.map:             # if key not exist 
                return False
            node = self.map[key]
            node.value = value              # else update value
            node.last_accessed = time.time() 
            if ttl is not None:
                node.expiry = time.time() + ttl
            self.dll.move_to_tail(node)     # move to MRU
            return True

    def delete(self, key):
        with self.lock:
            if key not in self.map:          # if key not exist 
                return False
            node = self.map[key]
            self.dll.remove_node(node)         #remove from DLL    
            del self.map[key]                   #remove from map 
            return True

    def list_keys(self):
        with self.lock:
            keys = []
            current = self.dll.head
            while current :
                if not self._is_expired(current):
                    keys.append(current.key)
                current = current.next
            return keys
    
    def dump(self):
        with self.lock:
            data = {}
            now = time.time()
            current = self.dll.head
            while current:
                if not self._is_expired(current):
                    ttl_left = int(current.expiry - now) if current.expiry else None
                    data[current.key] = {
                        "value": current.value,
                        "ttl": ttl_left,
                        "last_accessed" : current.last_accessed
                    }
                current = current.next
            return data
