package comiam;


import java.io.Serial;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;


/**
 * ElasticHashMap is an implementation of the Map interface based on the elastic hashing algorithm
 * described in the paper "Optimal Bounds for Open Addressing Without Reordering".
 *
 * <p>The table is divided into segments (A1, A2, ...). Batch 0 fills A1 until about 75% full.
 * For batches i >= 1, two segments are used: the current segment and the next.
 * Depending on the free fraction in the current segment (epsilon1) and the next (epsilon2),
 * one of three cases is used:
 * <ul>
 *   <li>Case 1: If epsilon1 > delta/2 and epsilon2 > 0.25, then first try limited probing in the current
 *       segment (up to f(epsilon1) probes) and, if unsuccessful, probe in the next segment.</li>
 *   <li>Case 2: If epsilon1 <= delta/2, force insertion in the next segment with linear probing.</li>
 *   <li>Case 3: If epsilon2 <= 0.25, force insertion in the current segment using linear probing.
 *       This case is rare.</li>
 * </ul>
 * </p>
 *
 * <p>This implementation strictly follows the ideas from the article.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public class ElasticHashMap<K, V> implements Map<K, V>, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final double LOG_OF_2 = Math.log(2);

    // Total capacity of the table.
    private int totalCapacity;

    // The load-gap parameter (0 < delta < 1); for simplicity, 1/delta should be a power of two.
    private final double delta;

    // Maximum number of insertions allowed.
    private int maxSize;

    // Threshold for resizing (equal to maxSize).
    private int threshold;

    // Array of segments (subarrays of the overall table).
    private Segment<K, V>[] segments;

    // Current batch number. Batch 0 uses only A1; batches >= 1 use two segments.
    private int currentBatch;

    // Current number of entries.
    private int size;

    // Multiplier constant for the probe limit: f(epsilon) = PROBE_MULTIPLIER * min(log2(1/epsilon), log2(1/delta)).
    private static final int PROBE_MULTIPLIER = 4;

    // Initial fill ratio for batch 0.
    private static final double INITIAL_FILL_RATIO = 0.75;

    // Flag used during resize to force rehash insertions into batch 0.
    private boolean rehashMode = false;

    // Pre-calculate logarithm of delta at construction time
    private final double logDeltaInverse;

    /**
     * Constructs an ElasticHashMap with the specified initial capacity and delta.
     *
     * @param initialCapacity the initial capacity of the table
     * @param delta           the load-gap parameter (0 < delta < 1)
     */
    public ElasticHashMap(int initialCapacity, double delta) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be positive");
        }
        if (delta <= 0 || delta >= 1) {
            throw new IllegalArgumentException("delta must be in (0,1)");
        }
        this.delta = delta;
        this.logDeltaInverse = Math.log(1.0 / delta) / LOG_OF_2;
        initializeTable(initialCapacity);
    }

    // Initializes table parameters and creates segments.
    private void initializeTable(final int capacity) {
        totalCapacity = capacity;
        maxSize = capacity - (int) Math.floor(delta * capacity);
        threshold = maxSize;
        currentBatch = 0;
        size = 0;
        segments = createSegments(capacity);
    }

    /**
     * Creates the segments array. The number of segments is floor(log2(capacity)) + 1.
     * Each segment's capacity is computed once and stored.
     *
     * @param capacity overall table capacity.
     * @return array of segments.
     */
    @SuppressWarnings("unchecked")
    private Segment<K, V>[] createSegments(final int capacity) {
        final double log2Capacity = Math.log(capacity) / LOG_OF_2;
        final int numSegments = (int) Math.floor(log2Capacity) + 1;
        Segment<K, V>[] segs = (Segment<K, V>[]) new Segment[numSegments];
        int sum = 0;

        for (int i = 0; i < numSegments; i++) {
            // Make each segment capacity a power of two for fast modulo
            final int powerOfTwo = Integer.highestOneBit(capacity >> (i + 1));
            final int segCap = Math.max(2, powerOfTwo);
            segs[i] = new Segment<>(segCap);
            sum += segCap;
        }

        // Adjust first segment to reach target capacity (make it power of two as well)
        if (sum < capacity) {
            int newCap = Integer.highestOneBit(segs[0].capacity + (capacity - sum)) * 2;
            segs[0].resize(newCap);
        }

        return segs;
    }

    /**
     * Entry holds a key-value pair along with insertion metadata.
     */
    private static final class Entry<K, V> {

        final K key;
        V value;
        final int hash; // Store hash code to avoid recalculation
        int segmentIndex;
        int probeCount;

        Entry(final K key, final V value, final int segmentIndex, final int probeCount) {
            this.key = key;
            this.hash = key.hashCode();
            this.value = value;
            this.segmentIndex = segmentIndex;
            this.probeCount = probeCount;
        }
    }

    /**
     * Segment represents a contiguous subarray of the table.
     * Its capacity is fixed at creation (but may be resized during table resize).
     */
    private static final class Segment<K, V> {

        Entry<K, V>[] table;
        int capacity;
        int count; // Number of occupied slots.

        @SuppressWarnings("unchecked")
        Segment(final int requestedCapacity) {
            // Round up to power of two
            this.capacity = Integer.highestOneBit(requestedCapacity - 1) << 1;
            if (this.capacity < requestedCapacity) {
                this.capacity <<= 1;
            }

            this.count = 0;
            this.table = (Entry<K, V>[]) new Entry[capacity];
        }

        void resize(final int newCapacity) {
            table = Arrays.copyOf(table, newCapacity);
            capacity = newCapacity;
        }

        /**
         * Returns the fraction of free slots in the segment.
         *
         * @return free fraction.
         */
        double freeFraction() {
            return 1.0 - ((double) count / capacity);
        }

        /**
         * Inserts the entry at the specified index, assuming the slot is free.
         *
         * @param index the index to insert.
         * @param entry the entry to insert.
         */
        void insert(final int index, final Entry<K, V> entry) {
            if (table[index] == null) {
                table[index] = entry;
                count++;
            }
        }

        /**
         * Deletes the entry at the specified index.
         *
         * @param index the index to delete.
         */
        void delete(final int index) {
            if (table[index] != null) {
                table[index] = null;
                count--;
            }
        }
    }

    // ---------------------- Hash and Probe Functions ----------------------

    /**
     * Mixes the hash code to produce a well-distributed result.
     *
     * @param h original hash code.
     * @return mixed hash code.
     */
    private int mix(final int h) {
        // More efficient mixing based on HashMap's implementation
        int hash = h;
        hash ^= (hash >>> 16);
        return hash;
    }

    /**
     * Computes the probe function: mix(hash) + segIdx * (probeCount)^2.
     *
     * @param segIdx     segment index.
     * @param probeCount probe count.
     * @param hash       precomputed hash code.
     * @return probe value.
     */
    private int probeFunction(final int segIdx, final int probeCount, final int hash) {
        return (mix(hash) + segIdx * probeCount * probeCount) & 0x7fffffff;
    }

    /**
     * Computes the index within a segment using precomputed values.
     * The key's hash is computed only once per call.
     *
     * @param seg        the segment.
     * @param segIdx     segment index.
     * @param probeCount probe count.
     * @param key        the key.
     * @return index within the segment.
     */
    private int probeIndex(final Segment<K, V> seg, final int segIdx, final int probeCount, final K key) {
        final int h = key.hashCode();
        final int probeFn = probeFunction(segIdx, probeCount, h);

        // Fast path for power-of-two capacities
        if ((seg.capacity & (seg.capacity - 1)) == 0) {
            return probeFn & (seg.capacity - 1);
        }

        // Fall back to modulo for non-power-of-two
        return probeFn % seg.capacity;
    }

    /**
     * Computes the maximum number of probes to attempt in a segment.
     * Uses precomputed logarithms to avoid repeated Math.log and Math.ceil calls.
     *
     * @param freeFraction free fraction of the segment.
     * @return maximum number of probe attempts.
     */
    private int probeLimit(final double freeFraction) {
        // Avoid the expensive log calculation for nearly full segments
        if (freeFraction <= 0.01) {
            return 32; // Reasonable upper bound
        }

        // Use a lookup table for common free fractions
        if (freeFraction >= 0.5) return 2;
        if (freeFraction >= 0.25) return 4;
        if (freeFraction >= 0.125) return 8;

        final double logFree = Math.log(1.0 / freeFraction) / LOG_OF_2;
        return (int) (PROBE_MULTIPLIER * Math.min(logFree, logDeltaInverse));
    }

    // ---------------------- Insertion Methods ----------------------

    /**
     * Inserts an entry using the elastic hashing algorithm.
     *
     * @param entry the entry to insert.
     */
    private void insertEntryInternal(final Entry<K, V> entry) {
        final K key = entry.key;

        // For batch 0, if not in rehash mode and A1 is near full, switch to batch 1.
        if (!rehashMode && currentBatch == 0) {
            final Segment<K, V> seg0 = segments[0];
            final int threshA1 = (int) Math.ceil(INITIAL_FILL_RATIO * seg0.capacity);
            if (seg0.count >= threshA1 && segments.length > 1) {
                currentBatch = 1;
            }
        }

        if (currentBatch == 0) {
            final Segment<K, V> seg = segments[0];
            final double epsilon = seg.freeFraction();
            final int limit = probeLimit(epsilon);
            // First try limited probing.
            for (int j = 1; j <= limit; j++) {
                if (insertEntryInternalImpl(entry, key, seg, j)) {
                    return;
                }
            }
            // Then try full linear probing.
            final int segCap = seg.capacity;
            for (int j = limit + 1; j <= segCap; j++) {
                if (insertEntryInternalImpl(entry, key, seg, j)) {
                    return;
                }
            }
            // If no free slot found in A1, resize and retry.
            resize();
            insertEntryInternal(entry);
        } else {
            final int b = currentBatch;
            final Segment<K, V> segB = segments[b];
            final Segment<K, V> segBplus = (b + 1 < segments.length) ? segments[b + 1] : null;
            final double epsilonB = segB.freeFraction();
            final double epsilonBplus = (segBplus != null) ? segBplus.freeFraction() : 1.0;
            if (epsilonB > delta / 2.0 && epsilonBplus > 0.25 && segBplus != null) {
                final int limit = probeLimit(epsilonB);
                for (int j = 1; j <= limit; j++) {
                    if (insertEntryInternalImpl(entry, key, b, segB, j)) {
                        return;
                    }
                }
                if (insertEntryInternalImpl(entry, key, b, segBplus)) {
                    return;
                }
                throw new IllegalStateException("Insertion failed in segments " + b + " and " + (b + 1));
            } else if (epsilonB <= delta / 2.0 && segBplus != null) {
                if (insertEntryInternalImpl(entry, key, b, segBplus)) {
                    return;
                }
                throw new IllegalStateException("Insertion failed in Segment " + (b + 1));
            } else if (epsilonBplus <= 0.25) {
                final int cap = segB.capacity;
                for (int j = 1; j <= cap; j++) {
                    if (insertEntryInternalImpl(entry, key, b, segB, j)) {
                        return;
                    }
                }
                throw new IllegalStateException("Insertion failed in Segment " + b);
            } else {
                final int cap = segB.capacity;
                for (int j = 1; j <= cap; j++) {
                    if (insertEntryInternalImpl(entry, key, b, segB, j)) {
                        return;
                    }
                }
                throw new IllegalStateException("Insertion failed in Segment " + b);
            }
        }
    }

    /**
     * Attempts to insert an entry into segment A1 (batch 0) with a given probe count.
     *
     * @param entry the entry.
     * @param key   the key.
     * @param seg   segment A1.
     * @param j     probe count.
     * @return true if insertion succeeds.
     */
    private boolean insertEntryInternalImpl(final Entry<K, V> entry, final K key,
                                            final Segment<K, V> seg, final int j) {
        final int idx = probeIndex(seg, 0, j, key);
        if (seg.table[idx] == null) {
            entry.segmentIndex = 0;
            entry.probeCount = j;
            seg.insert(idx, entry);
            size++;
            final int threshA1 = (int) Math.ceil(INITIAL_FILL_RATIO * seg.capacity);
            if (seg.count >= threshA1 && segments.length > 1) {
                currentBatch = 1;
            }
            return true;
        }
        return false;
    }

    /**
     * For batch i >= 1: attempts insertion using linear probing in a segment.
     *
     * @param entry the entry.
     * @param key   the key.
     * @param b     current batch index.
     * @param seg   the segment.
     * @return true if insertion succeeds.
     */
    private boolean insertEntryInternalImpl(final Entry<K, V> entry, final K key,
                                            final int b, final Segment<K, V> seg) {
        final int cap = seg.capacity;
        for (int j = 1; j <= cap; j++) {
            if (insertEntryInternalImpl(entry, key, b, seg, j)) {
                return true;
            }
        }
        return false;
    }

    /**
     * For batch i >= 1: attempts insertion with a specific probe count j.
     *
     * @param entry the entry.
     * @param key   the key.
     * @param b     current batch index.
     * @param seg   the segment.
     * @param j     probe count.
     * @return true if insertion succeeds.
     */
    private boolean insertEntryInternalImpl(final Entry<K, V> entry, final K key,
                                            final int b, final Segment<K, V> seg, final int j) {
        final int idx = probeIndex(seg, b, j, key);
        if (seg.table[idx] == null) {
            entry.segmentIndex = b;
            entry.probeCount = j;
            seg.insert(idx, entry);
            size++;
            updateBatchIfNeeded();
            return true;
        }
        return false;
    }

    /**
     * Updates the current batch based on segment fill levels.
     * Uses local caching to reduce field accesses.
     */
    private void updateBatchIfNeeded() {
        if (currentBatch == 0) {
            final Segment<K, V> seg = segments[0];
            final int threshA1 = (int) Math.ceil(INITIAL_FILL_RATIO * seg.capacity);
            if (seg.count >= threshA1 && segments.length > 1) {
                currentBatch = 1;
            }
        } else {
            final Segment<K, V> seg = segments[currentBatch];
            final int target = seg.capacity - (int) Math.floor(delta * seg.capacity / 2.0);
            if (seg.count >= target && currentBatch + 1 < segments.length) {
                currentBatch++;
            }
        }
    }

    /**
     * Searches for the key and, if found, updates its value.
     *
     * @param key      the key.
     * @param newValue the new value.
     * @return previous value if found, else null.
     */
    private V internalGetAndUpdate(final K key, final V newValue) {
        final int maxSeg = Math.min(segments.length, currentBatch + 2);
        for (int segIdx = 0; segIdx < maxSeg; segIdx++) {
            final Segment<K, V> seg = segments[segIdx];
            final int cap = seg.capacity;
            for (int j = 1; j <= cap; j++) {
                final int idx = probeIndex(seg, segIdx, j, key);
                final Entry<K, V> e = seg.table[idx];
                if (e == null) {
                    break;
                }
                if (e.key.equals(key)) {
                    final V oldVal = e.value;
                    e.value = newValue;
                    return oldVal;
                }
            }
        }
        return null;
    }

    /**
     * Associates the specified value with the specified key.
     *
     * @param key   the key.
     * @param value the value.
     * @return previous value or null.
     */
    @Override
    public V put(final K key, final V value) {
        Objects.requireNonNull(key, "Key cannot be null");
        final V old = internalGetAndUpdate(key, value);
        if (old != null) {
            return old;
        }
        if (size >= threshold) {
            resize();
        }
        final Entry<K, V> newEntry = new Entry<>(key, value, -1, -1);
        insertEntryInternal(newEntry);
        return null;
    }

    // ---------------------- Fast Rehash Insertion ----------------------

    /**
     * In a new, almost empty table, the first probe in A1 is nearly always free.
     * Precomputes index using j = 1.
     *
     * @param entry the entry to insert.
     */
    private void fastRehashInsert(final Entry<K, V> entry) {
        final Segment<K, V> seg = segments[0];
        final int idx = probeIndex(seg, 0, 1, entry.key);
        if (seg.table[idx] == null) {
            entry.segmentIndex = 0;
            entry.probeCount = 1;
            seg.insert(idx, entry);
            size++;
        } else {
            // Fallback rare path.
            insertEntryInternal(entry);
        }
    }

    // ---------------------- Resizing ----------------------

    /**
     * Resizes the table by doubling its capacity and rehashing all entries.
     *
     * @throws IllegalStateException if rehashing fails.
     */
    private void resize() {
        resize(totalCapacity * 2);
    }

    /**
     * Resizes the table by doubling its capacity and rehashing all entries.
     *
     * @param newCapacity the new capacity.
     * @throws IllegalStateException if rehashing fails.
     */
    private void resize(final int newCapacity) {
        final Segment<K, V>[] newSegments = createSegments(newCapacity);
        final Segment<K, V>[] oldSegments = segments;
        totalCapacity = newCapacity;
        maxSize = newCapacity - (int) Math.floor(delta * newCapacity);
        threshold = maxSize;
        currentBatch = 0;
        final int oldSize = size;
        size = 0;
        segments = newSegments;
        rehashMode = true;
        // Rehash entries from each old segment using fast-path method.
        for (final Segment<K, V> oldSeg : oldSegments) {
            final int cap = oldSeg.capacity;
            for (int i = 0; i < cap; i++) {
                final Entry<K, V> e = oldSeg.table[i];
                if (e != null) {
                    fastRehashInsert(e);
                }
            }
        }
        rehashMode = false;
        if (size != oldSize) {
            throw new IllegalStateException("Rehashing error: inconsistent size after resize");
        }
    }

    // ---------------------- Public Map Methods ----------------------

    /**
     * Returns the value associated with the specified key, or null if not found.
     *
     * @param key the key.
     * @return associated value or null.
     */
    @Override
    public V get(final Object key) {
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }

        // Compute hash code once
        final int hashCode = key.hashCode();

        // Fast path: check only first few slots in segment 0
        final Segment<K, V> firstSegment = segments[0];
        final int firstSegCap = firstSegment.capacity;
        final Entry<K, V>[] firstTable = firstSegment.table;
        final boolean isPowerOfTwo = (firstSegCap & (firstSegCap - 1)) == 0;
        final int mask = firstSegCap - 1;

        // Direct lookup using first probe (most common case)
        final int mixedHashCode = mix(hashCode);
        int idx = mixedHashCode & 0x7fffffff;
        if (isPowerOfTwo) {
            idx &= mask;
        } else {
            idx %= firstSegCap;
        }

        Entry<K, V> e = firstTable[idx];
        if (e != null && e.hash == hashCode && key.equals(e.key)) {
            return e.value;
        }

        // Try a few more probes in first segment with unrolled loop
        for (int probeCount = 2; probeCount <= 4; probeCount++) {
            idx = (mixedHashCode + probeCount * probeCount) & 0x7fffffff;
            if (isPowerOfTwo) {
                idx &= mask;
            } else {
                idx %= firstSegCap;
            }

            e = firstTable[idx];
            if (e == null) break;  // Early termination
            if (e.hash == hashCode && key.equals(e.key)) {
                return e.value;
            }
        }

        // If not found in the hot path, search remaining segments
        return searchRemainingSegments(key, hashCode, mixedHashCode);
    }

    /**
     * Searches for the key in remaining segments.
     * Separate method to keep hot path clean for JIT optimization.
     *
     * @param key           the key.
     * @param hashCode      the precomputed hash code.
     * @param mixedHashCode the mixed hash code.
     * @return associated value or null.
     */
    private V searchRemainingSegments(Object key, final int hashCode, final int mixedHashCode) {
        final int maxSeg = Math.min(segments.length, currentBatch + 2);

        // Skip segment 0 since we already checked it
        for (int segIdx = 1; segIdx < maxSeg; segIdx++) {
            final Segment<K, V> seg = segments[segIdx];
            final Entry<K, V>[] table = seg.table;
            final int capacity = seg.capacity;
            final boolean isPowerOfTwo = (capacity & (capacity - 1)) == 0;
            final int mask = capacity - 1;

            // Only check first 8 probes in each segment (diminishing returns after that)
            for (int probeCount = 1; probeCount <= 8; probeCount++) {
                final int probeFn = (mixedHashCode + segIdx * probeCount * probeCount) & 0x7fffffff;
                final int idx = isPowerOfTwo ? (probeFn & mask) : (probeFn % capacity);

                final Entry<K, V> e = table[idx];
                if (e == null) break; // Early termination
                if (e.hash == hashCode && key.equals(e.key)) {
                    return e.value;
                }
            }
        }
        return null;
    }

    /**
     * Removes the mapping for the specified key, if present.
     *
     * @param key the key.
     * @return previous value or null.
     */
    @Override
    @SuppressWarnings("unchecked")
    public V remove(final Object key) {
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }
        final int maxSeg = Math.min(segments.length, currentBatch + 2);
        for (int segIdx = 0; segIdx < maxSeg; segIdx++) {
            final Segment<K, V> seg = segments[segIdx];
            final int cap = seg.capacity;
            for (int j = 1; j <= cap; j++) {
                final int idx = probeIndex(seg, segIdx, j, (K) key);
                final Entry<K, V> e = seg.table[idx];
                if (e == null) {
                    break;
                }
                if (e.key.equals(key)) {
                    final V oldVal = e.value;
                    seg.delete(idx);
                    size--;
                    return oldVal;
                }
            }
        }
        return null;
    }

    /**
     * Returns true if this map contains a mapping for the specified key.
     *
     * @param key the key.
     * @return true if key is present.
     */
    @Override
    public boolean containsKey(final Object key) {
        return get(key) != null;
    }

    /**
     * Returns true if this map maps one or more keys to the specified value.
     *
     * @param value the value.
     * @return true if value is present.
     */
    @Override
    public boolean containsValue(final Object value) {
        for (final Segment<K, V> seg : segments) {
            for (final Entry<K, V> e : seg.table) {
                if (e != null && Objects.equals(e.value, value)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Copies all mappings from the specified map.
     *
     * @param m the map.
     */
    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        ensureCapacity(size + m.size());
        for (final Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity > threshold) {
            int newCapacity = Math.max(totalCapacity * 2,
                    (minCapacity * 4) / 3);
            // Perform a single resize operation
            resize(newCapacity);
        }
    }

    /**
     * Removes all mappings.
     */
    @Override
    public void clear() {
        for (final Segment<K, V> seg : segments) {
            Arrays.fill(seg.table, null);
            seg.count = 0;
        }
        size = 0;
        currentBatch = 0;
    }

    /**
     * Returns the number of key-value mappings.
     *
     * @return size of the map.
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * Returns true if the map is empty.
     *
     * @return true if empty.
     */
    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    // ---------------------- View Collections ----------------------

    private transient Set<Map.Entry<K, V>> entrySet;

    /**
     * Returns a Set view of the mappings.
     *
     * @return set of entries.
     */
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new AbstractSet<>() {
                @Override
                public Iterator<Map.Entry<K, V>> iterator() {
                    return new Iterator<>() {
                        int segIdx = 0;
                        int pos = 0;
                        Entry<K, V> nextEntry = advance();

                        // Advances to the next non-null entry.
                        private Entry<K, V> advance() {
                            while (segIdx < segments.length) {
                                final Segment<K, V> seg = segments[segIdx];
                                final int cap = seg.capacity;
                                while (pos < cap) {
                                    final Entry<K, V> e = seg.table[pos++];
                                    if (e != null) {
                                        return e;
                                    }
                                }
                                segIdx++;
                                pos = 0;
                            }
                            return null;
                        }

                        @Override
                        public boolean hasNext() {
                            return nextEntry != null;
                        }

                        @Override
                        public Map.Entry<K, V> next() {
                            if (nextEntry == null) {
                                throw new NoSuchElementException();
                            }
                            final Map.Entry<K, V> ret = new AbstractMap.SimpleImmutableEntry<>(nextEntry.key, nextEntry.value);
                            nextEntry = advance();
                            return ret;
                        }
                    };
                }

                @Override
                public int size() {
                    return ElasticHashMap.this.size;
                }
            };
        }
        return entrySet;
    }

    /**
     * Returns a Set view of the keys.
     *
     * @return set of keys.
     */
    @Override
    public Set<K> keySet() {
        final Set<K> ks = new HashSet<>();
        for (final Map.Entry<K, V> e : entrySet()) {
            ks.add(e.getKey());
        }
        return ks;
    }

    /**
     * Returns a Collection view of the values.
     *
     * @return collection of values.
     */
    @Override
    public Collection<V> values() {
        final List<V> vals = new ArrayList<>();
        for (final Map.Entry<K, V> e : entrySet()) {
            vals.add(e.getValue());
        }
        return vals;
    }

    /**
     * Compares the specified object with this map for equality.
     *
     * @param o the object.
     * @return true if equal.
     */
    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Map<?, ?> m)) {
            return false;
        }
        if (m.size() != size) {
            return false;
        }
        try {
            for (final Map.Entry<K, V> e : entrySet()) {
                final K key = e.getKey();
                final V value = e.getValue();
                if (!Objects.equals(value, m.get(key))) {
                    return false;
                }
            }
        } catch (final ClassCastException | NullPointerException unused) {
            return false;
        }
        return true;
    }

    /**
     * Returns the hash code value for this map.
     *
     * @return hash code.
     */
    @Override
    public int hashCode() {
        int h = 0;
        for (final Map.Entry<K, V> e : entrySet()) {
            h += e.hashCode();
        }
        return h;
    }

    /**
     * Returns a string representation of this map.
     *
     * @return string representation.
     */
    @Override
    public String toString() {
        final Iterator<Map.Entry<K, V>> i = entrySet().iterator();
        if (!i.hasNext()) {
            return "{}";
        }
        final StringBuilder sb = new StringBuilder();
        sb.append('{');
        while (true) {
            final Map.Entry<K, V> e = i.next();
            sb.append(e.getKey()).append('=').append(e.getValue());
            if (!i.hasNext()) {
                return sb.append('}').toString();
            }
            sb.append(", ");
        }
    }
}
