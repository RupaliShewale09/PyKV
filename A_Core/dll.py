class Node:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.prev = None
        self.next = None

class DoublyLinkedList:
    def __init__(self):
        self.head = None     #LRU
        self.tail = None     #MRU

    def add_to_tail(self, node):   # adds new node at MRU - when key inserted
        if not self.tail:
            self.head = self.tail = node
            return
        self.tail.next = node
        node.prev = self.tail
        self.tail = node

    def move_to_tail(self, node):   # moves existing node to MRU - get or update
        if node == self.tail:
            return

        if node.prev:
            node.prev.next = node.next
        else:
            self.head = node.next

        if node.next:
            node.next.prev = node.prev

        node.prev = self.tail
        node.next = None
        self.tail.next = node
        self.tail = node

    def remove_head(self):   # removes LRU node - when capacity exceed
        if not self.head:
            return None

        node = self.head
        self.head = node.next

        if self.head:
            self.head.prev = None
        else:
            self.tail = None

        node.prev = node.next = None
        return node

    def remove_node(self, node):   # remve specific node -- delete op
        if node == self.head:
            return self.remove_head()

        if node == self.tail:
            self.tail = node.prev
            if self.tail:
                self.tail.next = None
            else:
                self.head = None
            node.prev = None
            return node


        node.prev.next = node.next
        node.next.prev = node.prev
        node.prev = node.next = None
        return node
    
    def list_from_head(self):   # to display nodes from LRU i.e Head
        keys = []
        current = self.head
        while current:
            keys.append(current.key)
            current = current.next
        return keys
