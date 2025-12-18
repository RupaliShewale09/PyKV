from .dll import Node, DoublyLinkedList
from threading import Lock

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

    def get(self, key):     
        with self.lock:
            if key not in self.map:         # key not found -> None
                self.misses += 1
                return None
            
            self.hits +=1
            node = self.map[key]            
            self.dll.move_to_tail(node)       # move to MRU
            return node.value

    def set(self, key, value):
        with self.lock:
            if key in self.map:        #If key exists → reject insert 
                return False

            node = Node(key, value)         
            self.dll.add_to_tail(node)      # else add to MRU
            self.map[key] = node

            if len(self.map) > self.capacity:          # if capacity exceed
                lru = self.dll.remove_head()        # remove LRU
                if lru:
                    del self.map[lru.key]
                    self.evictions += 1
            return True

    def update(self, key, value):
        with self.lock:
            if key not in self.map:             # if key not exist 
                return False
            node = self.map[key]
            node.value = value              # else update value
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
            return self.dll.list_from_head()
