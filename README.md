# Key-Value Store Project

## Overview
This project implements a scalable and efficient Key-Value Store system in Python. The system supports in-memory storage and persistent disk-based storage using optimized data structures and algorithms, such as AVL Trees, Bloom Filters, and Sorted String Table (SST) files. It is designed to handle large-scale datasets while ensuring fast insertions, lookups, and compact storage.

## Features
- **In-Memory Storage:** Utilizes AVL Trees for balanced, efficient data storage and retrieval.
- **Persistent Storage:** Uses SST files for disk-based storage with Sparse Indexing for faster lookups.
- **Bloom Filters:** Reduces unnecessary disk I/O by quickly checking for key existence.
- **Automatic Compaction:** Merges multiple SST files, removing redundant data and improving performance.
- **Scalability:** Efficient handling of large datasets with minimal resource consumption.

## System Requirements
- Python 3.8 or higher
- Dependencies:
  - None (Custom implementation of Bloom Filters and AVL Trees removes reliance on external libraries)

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/OumaymaMel/K_V-store-Dictionary.git
   ```
2. Navigate to the project directory:
   ```bash
   cd K_V-store-Dictionary
   ```

## Usage
### Inserting Key-Value Pairs
You can insert key-value pairs into the Key-Value Store:
```python
from kvstore import KeyValueStore

store = KeyValueStore(memory_threshold=5)
store.insert("key1", "value1")
store.insert("key2", "value2")
```

### Retrieving Values
Retrieve values for a given key:
```python
value = store.get("key1")
print(value)  # Output: value1
```

### Triggering Compaction
Manually trigger file compaction:
```python
store.compact_sst_files()
```

## Testing
The project includes a comprehensive test suite to validate all functionalities:
1. Run the tests:
   ```bash
   python -m unittest discover tests
   ```
2. Example test cases include:
   - Insertion and retrieval of keys
   - Handling non-existing keys
   - SST file creation and compaction
   - Bloom Filter optimization

## Benchmarks
- **Insertion Time:** ~60 seconds for 10,000 keys.
- **Lookup Time (Existing Keys):** O(log n) for AVL Tree + O(1) for Bloom Filter.
- **Lookup Time (Non-Existing Keys):** Optimized by Bloom Filters with O(1) checks.
- **Compaction Time:** Efficient merging within 1-2 seconds.

## Project Structure
```
K_V-store-Dictionary/
├── kvstore.py         # Main implementation
├── tests/             # Test suite
├── data_store_db/     # Directory for SST files
├── README.md          # Documentation
└── requirements.txt   # Optional dependencies (currently empty)
```

## Contributors
- **Oumaima Meliani**
- **Aicha El Felchaoui**

This project is a collaborative effort as part of a two-student team.

## Future Improvements
1. Implement a Red-Black Tree for better balancing of secondary in-memory structures.
2. Add a Time-to-Live (TTL) mechanism for automatic key expiration.
3. Introduce multi-threading for improved compaction and concurrency support.
4. Enable transaction support for atomic operations.


