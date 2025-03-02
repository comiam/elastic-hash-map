# ElasticHashMap

![Build Status](https://github.com/comiam/elastic-hash-map/actions/workflows/maven.yml/badge.svg)

**ElasticHashMap** is a Java implementation of the elastic hashing algorithm as described in the paper:

**"Optimal Bounds for Open Addressing Without Reordering"**  
*Martín Farach-Colton, Andrew Krapivin, William Kuszmaul*

This project implements the algorithm proposed in the paper, which provides an open-addressed hash table design without
reordering inserted elements. The design guarantees:

- **Amortized Expected Search Complexity:** O(1)
- **Worst-case Expected Insertion/Search Complexity:** O(log(1/δ))

The implementation achieves these bounds by dividing the hash table into segments with geometrically decreasing sizes
and inserting entries in batches. Batch 0 fills the first segment (A1) up to about 75% capacity, while subsequent
batches use two segments and employ a mix of limited and linear probing based on the free fraction of the segments.

---

## Features

- **Elastic Hashing Algorithm:**  
  The implementation follows the scheme outlined in the paper. The table is split into segments (A1, A2, ...) and
  supports batch insertion strategies.

- **Automatic Resizing:**  
  When the table reaches its load threshold, the capacity is doubled and all existing entries are rehashed into a new
  table.

- **Standard Map Interface:**  
  The class implements Java's `Map<K, V>` interface, including methods such as:
    - `put(K key, V value)`
    - `get(Object key)`
    - `remove(Object key)`
    - `containsKey(Object key)`
    - `containsValue(Object value)`
    - `clear()`
    - Collection views: `keySet()`, `values()`, `entrySet()`
    - Overridden `equals()`, `hashCode()`, and `toString()` methods for proper map behavior.

- **Performance Guarantees:**  
  The implementation is designed to ensure efficient search and insertion operations based on the theoretical analysis
  provided in the paper.

- **Code Quality:**  
  The code adheres to Google Java Style guidelines and includes detailed Javadoc comments (in English) explaining the
  purpose and behavior of each class and method.

---

## Project Structure

- **ElasticHashMap.java:**  
  The main class implementing the elastic hashing map. It contains the full implementation of the algorithm, including
  methods for insertion, deletion, lookup, and resizing.

- **Unit Tests:**  
  A set of JUnit tests (e.g., `ElasticHashMapTest.java`) are provided to verify the correctness of the implementation
  across various scenarios, including basic operations, edge cases, and resizing behavior.

---

## How It Works

1. **Segmentation & Batching:**
    - The table is divided into segments. The number of segments is determined by the logarithm (base 2) of the
      capacity.
    - In **Batch 0**, elements are inserted into the first segment (A1) until it reaches approximately 75% capacity.
    - In **Batches ≥ 1**, two segments are used. Depending on the free fraction (ε) of the current segment and the next
      segment, the algorithm chooses between:
        - **Limited probing in the current segment** (if ε > δ/2 and the next segment is sufficiently free),
        - **Linear probing in the next segment** (if ε ≤ δ/2), or
        - **Forced linear probing in the current segment** (if the next segment is too full).

2. **Probing Mechanism:**
    - The algorithm uses a two-dimensional probe sequence where the probe index is computed by mixing the key's hash
      with an offset that increases quadratically with the probe count and the segment index.
    - A probe limit function controls the maximum number of probes attempted in a nearly full segment, ensuring that
      insertion does not get stuck in the coupon collector's bottleneck.

3. **Resizing:**
    - When the overall size reaches a threshold, the map doubles its capacity.
    - During resizing, all existing entries are rehashed into a new set of segments following the elastic hashing
      strategy.

---

## Usage

To use the `ElasticHashMap`, simply include the class in your project and create an instance:

```java
import comiam.ElasticHashMap;

ElasticHashMap<String, Integer> map = new ElasticHashMap<>(1024, 0.125);
map.put("apple", 1);
Integer value = map.get("apple");
```

The class implements all standard operations defined in the `Map` interface.

---

## Testing

Unit tests are written using JUnit 5. To run the tests, use your favorite IDE or run the following Maven/Gradle command
if you are using a build tool:

```bash
mvn test
```

The tests cover basic insertion, updating, removal, resizing, and view collection methods. They also include edge cases
such as invalid parameters and null key insertions.

---

## License

This project is provided under the Mozilla Public License 2.0. See the [LICENSE](LICENSE) file for details.

---

## References

- **Paper:** "Optimal Bounds for Open Addressing Without Reordering"  
  [arXiv:2501.02305](http://arxiv.org/abs/2501.02305)

This implementation is intended for educational purposes and as a demonstration of advanced hashing techniques.
