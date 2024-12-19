import os
import pickle
import hashlib
import bisect
import gzip

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

# Sparse Index SST Manager
class SparseIndexSST:
    def __init__(self, directory, sparse_interval=3):
        self.directory = directory
        os.makedirs(directory, exist_ok=True)
        self.sparse_interval = sparse_interval
        self.file_counter = 0

    def dump_to_file(self, data):
        if not data:
            return
        filename = os.path.join(self.directory, f"F{self.file_counter}.sst")
        sparse_index = []
        position = 0

        with gzip.open(filename, "wb") as f:
            for i, (key, value) in enumerate(data):
                serialized = pickle.dumps((key, value))
                f.write(serialized)
                if i % self.sparse_interval == 0:
                    sparse_index.append((key, position))
                position += len(serialized)

            index_position = f.tell()
            pickle.dump(sparse_index, f)
            f.write(index_position.to_bytes(8, "big"))

        print(f"Dumped {len(data)} items to {filename} with sparse index.")
        self.file_counter += 1

    def get(self, key):
        for i in range(self.file_counter):
            filename = os.path.join(self.directory, f"F{i}.sst")
            try:
                with gzip.open(filename, "rb") as f:
                    f.seek(-8, os.SEEK_END)
                    index_position = int.from_bytes(f.read(8), "big")
                    f.seek(index_position)
                    sparse_index = pickle.load(f)
                    keys = [k for k, _ in sparse_index]
                    pos = bisect.bisect_left(keys, key)

                    start_position = sparse_index[max(0, pos - 1)][1]
                    f.seek(start_position)

                    while f.tell() < index_position:
                        try:
                            current_key, value = pickle.load(f)
                            if current_key == key:
                                print(f"Found in {filename}: {key} -> {value}")
                                return value
                        except EOFError:
                            break
            except (FileNotFoundError, EOFError):
                continue
        return None

# KeyValueStore Implementation
class KeyValueStore:
    def __init__(self, memory_threshold=5, database_path="data_store_db", sparse_interval=3):
        self.avl_tree = AVLTree()
        self.red_black_tree = {}
        self.sst_manager = SparseIndexSST(database_path, sparse_interval)
        self.memory_threshold = memory_threshold
        self.item_count = 0

    def insert(self, key, value):
        if self.item_count < self.memory_threshold:
            print(f"Inserting in AVL Tree: {key} -> {value}")
            self.avl_tree.insert(key, value)
        else:
            print(f"Inserting in Red-Black Tree: {key} -> {value}")
            self.red_black_tree[key] = value
            if len(self.red_black_tree) >= self.memory_threshold:
                self.dump_to_file()
        self.item_count += 1

    def dump_to_file(self):
        print("Dumping data to SST file...")
        self.sst_manager.dump_to_file(sorted(self.red_black_tree.items()))
        self.red_black_tree = {}

    def get(self, key):
        for k, v in self.avl_tree.in_order():
            if k == key:
                return v
        if key in self.red_black_tree:
            return self.red_black_tree[key]
        return self.sst_manager.get(key)

# Testing KeyValueStore
def test_key_value_store():
    print("--- Testing KeyValueStore ---")
    store = KeyValueStore(memory_threshold=5)

    for i in range(20):
        store.insert(f"key{i}", i)

    print("\nRetrieving existing keys:")
    for i in range(20):
        value = store.get(f"key{i}")
        print(f"key{i} -> {value}")

if __name__ == "__main__":
    test_key_value_store()
