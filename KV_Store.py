import pickle
import os
import hashlib
import time
import random
import string

# Custom Bloom Filter Implementation
class BloomFilter:
    def __init__(self, size=1000, hash_count=3):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = [0] * size

    def _hashes(self, key):
        return [int(hashlib.md5((key + str(i)).encode()).hexdigest(), 16) % self.size for i in range(self.hash_count)]

    def add(self, key):
        for hash_value in self._hashes(key):
            self.bit_array[hash_value] = 1

    def __contains__(self, key):
        return all(self.bit_array[hash_value] for hash_value in self._hashes(key))


# AVL Tree Node
class AVLNode:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.height = 1
        self.left = None
        self.right = None


# AVL Tree Implementation
class AVLTree:
    def __init__(self):
        self.root = None

    def _height(self, node):
        return node.height if node else 0

    def _balance_factor(self, node):
        return self._height(node.left) - self._height(node.right)

    def _rotate_left(self, z):
        y = z.right
        T2 = y.left
        y.left = z
        z.right = T2
        z.height = 1 + max(self._height(z.left), self._height(z.right))
        y.height = 1 + max(self._height(y.left), self._height(y.right))
        return y

    def _rotate_right(self, z):
        y = z.left
        T3 = y.right
        y.right = z
        z.left = T3
        z.height = 1 + max(self._height(z.left), self._height(z.right))
        y.height = 1 + max(self._height(y.left), self._height(y.right))
        return y

    def _insert(self, node, key, value):
        if not node:
            return AVLNode(key, value)
        if key < node.key:
            node.left = self._insert(node.left, key, value)
        elif key > node.key:
            node.right = self._insert(node.right, key, value)
        else:
            node.value = value
            return node

        node.height = 1 + max(self._height(node.left), self._height(node.right))
        balance = self._balance_factor(node)

        # Perform rotations if unbalanced
        if balance > 1 and key < node.left.key:
            return self._rotate_right(node)
        if balance < -1 and key > node.right.key:
            return self._rotate_left(node)
        if balance > 1 and key > node.left.key:
            node.left = self._rotate_left(node.left)
            return self._rotate_right(node)
        if balance < -1 and key < node.right.key:
            node.right = self._rotate_right(node.right)
            return self._rotate_left(node)

        return node

    def insert(self, key, value):
        self.root = self._insert(self.root, key, value)

    def _in_order(self, node):
        if node:
            yield from self._in_order(node.left)
            yield (node.key, node.value)
            yield from self._in_order(node.right)

    def in_order(self):
        return list(self._in_order(self.root))


# Simple Red-Black Tree (Placeholder)
class RedBlackTree:
    def __init__(self):
        self.data = {}

    def insert(self, key, value):
        self.data[key] = value

    def get_sorted_items(self):
        return sorted(self.data.items())


# Key-Value Store Implementation
class KeyValueStore:
    def __init__(self, memory_threshold=5, database_path="data_store_db"):
        self.avl_tree = AVLTree()
        self.red_black_tree = RedBlackTree()
        self.memory_threshold = memory_threshold
        self.database_path = database_path
        self.item_count = 0
        self.file_counter = 1
        self.metadata_file = os.path.join(database_path, "index.pkl")
        self.metadata = []

        # Ensure the database folder exists
        if not os.path.exists(self.database_path):
            os.makedirs(self.database_path)
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, "rb") as f:
                self.metadata = pickle.load(f)

    def insert(self, key, value):
        if self.item_count < self.memory_threshold:
            self.avl_tree.insert(key, value)
        else:
            self.red_black_tree.insert(key, value)
            if len(self.red_black_tree.data) >= self.memory_threshold:
                self.dump_to_file()
                self.red_black_tree = RedBlackTree()
        self.item_count += 1

    def dump_to_file(self):
        items = self.red_black_tree.get_sorted_items()
        file_name = os.path.join(self.database_path, f"F{self.file_counter}.pkl")
        bloom = BloomFilter(size=1000, hash_count=3)

        with open(file_name, "wb") as f:
            pickle.dump(items, f)
            for key, _ in items:
                bloom.add(key)

        self.metadata.append({
            "file": file_name,
            "start_key": items[0][0],
            "end_key": items[-1][0],
            "bloom_filter": bloom
        })

        with open(self.metadata_file, "wb") as f:
            pickle.dump(self.metadata, f)
        self.file_counter += 1

    def get(self, key):
        for k, v in self.avl_tree.in_order():
            if k == key:
                return v

        if key in self.red_black_tree.data:
            return self.red_black_tree.data[key]

        for file_meta in self.metadata:
            bloom = file_meta["bloom_filter"]
            if key in bloom:
                with open(file_meta["file"], "rb") as f:
                    items = pickle.load(f)
                    for k, v in items:
                        if k == key:
                            return v
        return None

def generate_random_key(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def test_large_data(store, num_entries=100):
    print(f"\nInserting {num_entries} random key-value pairs...")
    inserted_keys = []
    for _ in range(num_entries):
        key = generate_random_key()
        value = random.randint(1, 1000)
        store.insert(key, value)
        inserted_keys.append(key)

    print("\nTesting retrieval of inserted keys:")
    for key in inserted_keys[:5]:
        value = store.get(key)
        print(f"Key: {key} -> Value: {value}")

    print("\nTesting retrieval of random non-existent keys:")
    for _ in range(5):
        key = generate_random_key()
        value = store.get(key)
        print(f"Key: {key} -> Value: {value}")


if __name__ == "__main__":
    store = KeyValueStore(memory_threshold=10)
    test_large_data(store, num_entries=50)