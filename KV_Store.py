import os
import pickle
import hashlib
import bisect
import gzip
import logging
import shutil
import unittest
import time

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
        self.bloom_filters = []

    def dump_to_file(self, data):
        if not data:
            logger.warning("Attempted to dump empty data to SST file.")
            return

        filename = os.path.join(self.directory, f"F{self.file_counter}.sst")
        sparse_index = []
        bloom_filter = BloomFilter()
        position = 0

        try:
            with gzip.open(filename, "wb") as f:
                for i, (key, value) in enumerate(data):
                    serialized = pickle.dumps((key, value))
                    f.write(serialized)

                    bloom_filter.add(key)
                    if i % self.sparse_interval == 0:
                        sparse_index.append((key, position))
                    position += len(serialized)

                index_position = f.tell()
                pickle.dump(sparse_index, f)
                f.write(index_position.to_bytes(8, "big"))

            self.bloom_filters.append(bloom_filter)
            logger.info(f"Dumped {len(data)} items to {filename} with sparse index and Bloom Filter.")
        except Exception as e:
            logger.error(f"Error while dumping data to {filename}: {e}")
        self.file_counter += 1

    def get(self, key):
        for i in range(self.file_counter):
            if key not in self.bloom_filters[i]:
                continue
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
                        current_key, value = pickle.load(f)
                        if current_key == key:
                            logger.info(f"Found {key} in {filename}: {value}")
                            return value
            except (FileNotFoundError, EOFError, pickle.UnpicklingError):
                continue
        return None

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

    def compact_sst_files(self):
        print("\nStarting compaction...")
        all_data = {}
        for i in range(self.sst_manager.file_counter):
            filename = os.path.join(self.sst_manager.directory, f"F{i}.sst")
            try:
                with gzip.open(filename, "rb") as f:
                    f.seek(-8, os.SEEK_END)
                    index_position = int.from_bytes(f.read(8), "big")
                    f.seek(0)

                    while f.tell() < index_position:
                        key, value = pickle.load(f)
                        all_data[key] = value
                os.remove(filename)
                print(f"Deleted old SST file: {filename}")
            except FileNotFoundError:
                continue
                
        self.sst_manager.file_counter = 0
        self.sst_manager.bloom_filters = []

        # Dump the compacted data to a new SST file
        self.sst_manager.dump_to_file(sorted(all_data.items()))

        print("Compaction complete.")

class TestKeyValueStore(unittest.TestCase):
    TEST_DIR = "data_store_db"

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.TEST_DIR):
            shutil.rmtree(cls.TEST_DIR)
        os.makedirs(cls.TEST_DIR, exist_ok=True)
        cls.store = KeyValueStore(memory_threshold=5, database_path=cls.TEST_DIR, sparse_interval=3)


    def timed_test(func):
        """Decorator to measure the time of each test"""
        def wrapper(self, *args, **kwargs):
            start_time = time.time()
            func(self, *args, **kwargs)
            end_time = time.time()
            print(f"\nTest '{func.__name__}' completed in {end_time - start_time:.4f} seconds")
        return wrapper
  
    @timed_test  
    def test_1_insertion_and_retrieval(self):
        print("\n--- Test 1: Insertion and Retrieval ---")
        # Insert 20 key-value pairs
        for i in range(20):
            self.store.insert(f"key{i}", i)

        # Retrieve and validate existing keys
        for i in range(20):
            value = self.store.get(f"key{i}")
            self.assertEqual(value, i, f"key{i} should return value {i}, but got {value}")
            print(f"key{i} -> {value}")

    @timed_test
    def test_2_non_existing_keys(self):
        print("\n--- Test 2: Retrieval of Non-Existing Keys ---")
        # Retrieve keys that do not exist
        for i in range(20, 25):
            value = self.store.get(f"key{i}")
            self.assertIsNone(value, f"key{i} should return None, but got {value}")
            print(f"key{i} -> {value}")

    @timed_test
    def test_3_sst_file_creation(self):
        print("\n--- Test 3: SST File Creation and Retrieval ---")
        # Verify SST files exist
        sst_files = os.listdir(self.TEST_DIR)
        self.assertGreater(len(sst_files), 0, "SST files were not created.")
        print(f"SST files: {sst_files}")

        # Retrieve some keys stored in SST files
        for i in range(5, 15):
            value = self.store.get(f"key{i}")
            self.assertEqual(value, i, f"key{i} should return value {i}, but got {value}")
            print(f"key{i} -> {value}")

    @timed_test
    def test_4_compaction(self):
        print("\n--- Test 4: Compaction ---")
        # Trigger compaction
        self.store.compact_sst_files()

        # Verify only one SST file remains
        sst_files = os.listdir(self.TEST_DIR)
        self.assertEqual(len(sst_files), 1, "Compaction did not reduce SST files to one.")
        print(f"Remaining SST file after compaction: {sst_files}")

        # Verify all keys are still retrievable
        for i in range(20):
            value = self.store.get(f"key{i}")
            self.assertEqual(value, i, f"key{i} should return value {i}, but got {value}")
            print(f"key{i} -> {value}")
            
    @timed_test
    def test_5_reinsertion_after_compaction(self):
        print("\n--- Test 5: Reinsertion After Compaction ---")
        # Re-insert new keys
        for i in range(20, 25):
            self.store.insert(f"key{i}", i * 2)

        # Retrieve and verify new keys
        for i in range(20, 25):
            value = self.store.get(f"key{i}")
            self.assertEqual(value, i * 2, f"key{i} should return value {i * 2}, but got {value}")
            print(f"key{i} -> {value}")

    @timed_test
    def test_6_bloom_filter_optimization(self):
        print("\n--- Test 6: Bloom Filter Optimization ---")
        # Check that Bloom Filter prevents unnecessary SST file lookups
        nonexistent_key = "key100"
        value = self.store.get(nonexistent_key)
        self.assertIsNone(value, f"{nonexistent_key} should return None, but got {value}")
        print(f"{nonexistent_key} -> {value}")

    @timed_test
    def test_7_large_data_insertion_and_retrieval(self):
        print("\n--- Test 7: Large Data Insertion and Retrieval ---")
        
        # Initialize a store for large data testing
        large_store = KeyValueStore(memory_threshold=100, database_path=self.TEST_DIR, sparse_interval=10)
        
        NUM_KEYS = 10000  # Define the size of the large dataset
        
        # Insert 10,000 key-value pairs
        print(f"Inserting {NUM_KEYS} keys into the store...")
        for i in range(NUM_KEYS):
            large_store.insert(f"key{i}", i)

        # Verify random keys are retrievable
        print("Verifying random key retrieval...")
        for i in range(0, NUM_KEYS, 500):
            value = large_store.get(f"key{i}")
            self.assertEqual(value, i, f"key{i} should return value {i}, but got {value}")
            print(f"key{i} -> {value}")

        # Check the number of SST files before compaction
        sst_files = os.listdir(self.TEST_DIR)
        print(f"Total SST files before compaction: {len(sst_files)}")

        # Trigger manual compaction
        print("Triggering file compaction...")
        large_store.compact_sst_files()

        # Verify only one SST file remains after compaction
        sst_files_after = os.listdir(self.TEST_DIR)
        print(f"Total SST files after compaction: {len(sst_files_after)}")
        self.assertEqual(len(sst_files_after), 1, "There should be exactly 1 SST file after compaction.")

        # Verify all keys after compaction
        print("Verifying all keys after compaction...")
        for i in range(NUM_KEYS):
            value = large_store.get(f"key{i}")
            self.assertEqual(value, i, f"key{i} should return value {i}, but got {value}")
        
        print(f"All {NUM_KEYS} keys successfully verified after compaction.")

if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)
