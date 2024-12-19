import os
import pickle
import hashlib
import bisect
import gzip
import logging
import unittest
import shutil
import os

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


# Setup logging
logging.basicConfig(level=logging.INFO, filename="kvstore.log", format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

class SparseIndexSST:
    def __init__(self, directory, sparse_interval=3):
        self.directory = directory
        os.makedirs(directory, exist_ok=True)
        self.sparse_interval = sparse_interval
        self.file_counter = 0
        logger.info("SparseIndexSST initialized.")

    def dump_to_file(self, data):
        """
        Dumps sorted data to an SST file with a sparse index.
        """
        if not data:
            logger.warning("Attempted to dump empty data to SST file.")
            return
        
        filename = os.path.join(self.directory, f"F{self.file_counter}.sst")
        sparse_index = []
        position = 0

        try:
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
            logger.info(f"Dumped {len(data)} items to {filename} with sparse index.")
        except Exception as e:
            logger.error(f"Error while dumping data to {filename}: {e}")
        self.file_counter += 1

    def get(self, key):
        """
        Retrieve a key's value using the sparse index for optimized search.
        """
        for i in range(self.file_counter):
            filename = os.path.join(self.directory, f"F{i}.sst")
            try:
                with gzip.open(filename, "rb") as f:
                    # Read sparse index position
                    f.seek(-8, os.SEEK_END)
                    index_position = int.from_bytes(f.read(8), "big")

                    # Load sparse index
                    f.seek(index_position)
                    sparse_index = pickle.load(f)
                    keys = [k for k, _ in sparse_index]

                    # Perform binary search in the sparse index
                    pos = bisect.bisect_left(keys, key)
                    start_position = sparse_index[max(0, pos - 1)][1]
                    f.seek(start_position)

                    # Linear search from start_position
                    while f.tell() < index_position:
                        current_key, value = pickle.load(f)
                        if current_key == key:
                            logger.info(f"Found {key} in {filename}: {value}")
                            return value
            except (FileNotFoundError, EOFError, pickle.UnpicklingError) as e:
                logger.warning(f"Error reading {filename}: {e}")
        logger.info(f"Key {key} not found in any SST file.")
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
        # Check in AVL Tree
        for k, v in self.avl_tree.in_order():
            if k == key:
                return v
        
        # Check in Red-Black Tree
        if key in self.red_black_tree:
            return self.red_black_tree[key]

        # Check in SST files
        return self.sst_manager.get(key)

    def compact_sst_files(self):
        print("\nStarting compaction...")
        all_data = []
        for i in range(self.sst_manager.file_counter):
            filename = os.path.join(self.sst_manager.directory, f"F{i}.sst")
            try:
                with gzip.open(filename, "rb") as f:
                    f.seek(-8, os.SEEK_END)
                    index_position = int.from_bytes(f.read(8), "big")
                    f.seek(0)

                    while f.tell() < index_position:
                        try:
                            key, value = pickle.load(f)
                            all_data.append((key, value))
                        except EOFError:
                            break

                os.remove(filename)
                print(f"Deleted old SST file: {filename}")
            except FileNotFoundError:
                continue

        # Sort and dump the merged data into a new SST file
        self.sst_manager.file_counter = 0
        self.sst_manager.dump_to_file(sorted(all_data))
        print("Compaction complete.")


class TestSparseIndexSST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Clean up existing database folder
        cls.db_dir = "data_store_db"
        if os.path.exists(cls.db_dir):
            shutil.rmtree(cls.db_dir)
        os.makedirs(cls.db_dir, exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        # Remove test database folder after tests
        shutil.rmtree(cls.db_dir)

    def test_get_from_empty_store(self):
        sst = SparseIndexSST(self.db_dir)
        result = sst.get("key_not_exist")
        self.assertIsNone(result, "Retrieving from an empty store should return None.")

    def test_get_with_corrupted_file(self):
        sst = SparseIndexSST(self.db_dir)
        # Manually create a corrupted SST file
        filename = os.path.join(self.db_dir, "F0.sst")
        with open(filename, "wb") as f:
            f.write(b"corrupted_data")

        result = sst.get("key_not_exist")
        self.assertIsNone(result, "Retrieving from a corrupted SST file should return None.")

    def test_dump_and_retrieve(self):
        sst = SparseIndexSST(self.db_dir)
        data = [("key1", 1), ("key2", 2), ("key3", 3)]
        sst.dump_to_file(data)

        result = sst.get("key2")
        self.assertEqual(result, 2, "Retrieving an existing key should return its value.")
        result = sst.get("key_not_exist")
        self.assertIsNone(result, "Retrieving a non-existing key should return None.")

if __name__ == "__main__":
    # Run unittest in interactive environments
    unittest.main(argv=[''], exit=False)
