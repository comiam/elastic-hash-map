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
 * <p>The overall design uses a table that is divided into segments (Segment A1, A2, ...). The table is
 * filled in batches. Batch 0 fills Segment A1 until about 75% full. For batches i >= 1, two segments are
 * used: the current segment and the next segment. Depending on the free fraction in the current segment
 * (epsilon1) and the next segment (epsilon2), one of three cases is used:
 * <ul>
 *   <li>Case 1: If epsilon1 > delta/2 and epsilon2 > 0.25, then first try limited probing in the current segment,
 *       up to f(epsilon1) probes, and if unsuccessful, probe in the next segment.</li>
 *   <li>Case 2: If epsilon1 <= delta/2, then force insertion in the next segment with linear probing.</li>
 *   <li>Case 3: If epsilon2 <= 0.25, then force insertion in the current segment using linear probing.
 *       This case is rare.</li>
 * </ul>
 * </p>
 *
 * <p>This class implements all methods from the Map interface including equals, hashCode, and toString.</p>
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public class ElasticHashMap<K, V> implements Map<K, V>, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    // Total capacity of the table.
    private int totalCapacity;

    // The load-gap parameter (0 < delta < 1). For simplicity, 1/delta should be a power of two.
    private final double delta;

    // Maximum number of insertions allowed.
    private int maxSize;

    // Threshold for resizing (set to maxSize).
    private int threshold;

    // Array of segments; each segment is a subarray of the overall table.
    private Segment<K, V>[] segments;

    // Current batch number. Batch 0 uses only segment A1; batches >= 1 use two segments.
    private int currentBatch;

    // Current number of entries in the table.
    private int size;

    // Multiplier constant for the probe limit function: f(epsilon) = PROBE_MULTIPLIER * min(log2(1/epsilon), log2(1/delta)).
    private static final int PROBE_MULTIPLIER = 4;

    // Initial fill ratio for batch 0.
    private static final double INITIAL_FILL_RATIO = 0.75;

    // Flag used during resize to force rehash insertions into batch 0.
    private boolean rehashMode = false;

    /**
     * Constructs an ElasticHashMap with the given initial capacity and delta parameter.
     * The table will automatically resize (doubling its capacity) when the load threshold is reached.
     *
     * @param initialCapacity the initial overall capacity
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
        initializeTable(initialCapacity);
    }

    // Initializes the table parameters for a given capacity.
    private void initializeTable(int capacity) {
        totalCapacity = capacity;
        maxSize = capacity - (int) Math.floor(delta * capacity);
        threshold = maxSize;
        currentBatch = 0;
        size = 0;
        segments = createSegments(capacity);
    }

    @SuppressWarnings("unchecked")
    private Segment<K, V>[] createSegments(int capacity) {
        int numSegments = (int) (Math.floor(Math.log(capacity) / Math.log(2))) + 1;
        Segment<K, V>[] segs = (Segment<K, V>[]) new Segment[numSegments];
        int sum = 0;
        for (int i = 0; i < numSegments; i++) {
            int cap = Math.max(1, capacity >> (i + 1));
            segs[i] = new Segment<>(cap);
            sum += cap;
        }
        // Adjust the first segment so that the total capacity equals the provided capacity.
        if (sum < capacity) {
            segs[0].resize(segs[0].capacity + (capacity - sum));
        }
        return segs;
    }

    /**
     * Entry stores a key-value pair along with metadata about its insertion.
     */
    private static class Entry<K, V> {

        final K key;
        V value;
        int segmentIndex;
        int probeCount;

        Entry(K key, V value, int segmentIndex, int probeCount) {
            this.key = key;
            this.value = value;
            this.segmentIndex = segmentIndex;
            this.probeCount = probeCount;
        }
    }

    /**
     * Segment represents a subarray of the overall table.
     */
    private static class Segment<K, V> {

        Entry<K, V>[] table;
        int capacity;
        int count; // number of occupied slots

        @SuppressWarnings("unchecked")
        Segment(int capacity) {
            this.capacity = capacity;
            this.count = 0;
            this.table = (Entry<K, V>[]) new Entry[capacity];
        }

        void resize(int newCapacity) {
            table = Arrays.copyOf(table, newCapacity);
            capacity = newCapacity;
        }

        /**
         * Returns the free fraction of this segment.
         *
         * @return the fraction of free slots
         */
        double freeFraction() {
            return 1.0 - ((double) count / capacity);
        }

        void insert(int index, Entry<K, V> entry) {
            if (table[index] == null) {
                table[index] = entry;
                count++;
            }
        }

        void delete(int index) {
            if (table[index] != null) {
                table[index] = null;
                count--;
            }
        }
    }

    // ---------------------- Hash and Probe Functions ----------------------

    /**
     * Mixes the hash code to produce a well-distributed integer.
     *
     * @param h the original hash code
     * @return a mixed hash code
     */
    private int mix(int h) {
        h ^= (h >>> 16);
        return h;
    }

    /**
     * Computes the probe function for a given segment index and probe count.
     * This function grows approximately as segmentIndex * (probeCount)^2.
     *
     * @param segIdx     the segment index (0-based)
     * @param probeCount the current probe count
     * @param hash       the hash code of the key
     * @return a nonnegative integer for computing the slot index
     */
    private int probeFunction(int segIdx, int probeCount, int hash) {
        int deltaValue = segIdx * probeCount * probeCount;
        return (mix(hash) + deltaValue) & 0x7fffffff;
    }

    /**
     * Computes the index within a segment for a given key, segment index, and probe count.
     *
     * @param seg        the segment to probe
     * @param segIdx     the segment index (0-based)
     * @param probeCount the probe count
     * @param key        the key for insertion or search
     * @return the index within the segment
     */
    private int probeIndex(Segment<K, V> seg, int segIdx, int probeCount, K key) {
        return probeFunction(segIdx, probeCount, key.hashCode()) % seg.capacity;
    }

    /**
     * The probe limit function f(epsilon) = PROBE_MULTIPLIER * ceil(min(log2(1/epsilon), log2(1/delta))).
     * This limits the number of probes to attempt in a nearly full segment.
     *
     * @param freeFraction the fraction of free slots in the segment
     * @return the maximum number of probe attempts
     */
    private int probeLimit(double freeFraction) {
        if (freeFraction <= 0) {
            return Integer.MAX_VALUE;
        }
        double limit1 = Math.log(1.0 / freeFraction) / Math.log(2);
        double limit2 = Math.log(1.0 / delta) / Math.log(2);
        return (int) Math.ceil(PROBE_MULTIPLIER * Math.min(limit1, limit2));
    }

    // ---------------------- Insertion Methods ----------------------

    /**
     * Inserts an entry using the elastic hashing algorithm.
     *
     * @param entry the entry to insert
     */
    private void insertEntryInternal(Entry<K, V> entry) {
        K key = entry.key;

        // In normal mode (rehashMode==false), if A1 is at (or above) the threshold and there is more than one segment,
        // then switch to batch 1. In rehash mode we always insert into batch 0.
        if (!rehashMode && currentBatch == 0
                && segments[0].count >= Math.ceil(INITIAL_FILL_RATIO * segments[0].capacity)
                && segments.length > 1) {
            currentBatch = 1;
        }

        if (currentBatch == 0) {
            Segment<K, V> seg = segments[0];
            double epsilon = seg.freeFraction();
            int limit = probeLimit(epsilon);
            for (int j = 1; j <= limit; j++) {
                if (insertEntryInternalImpl(entry, key, seg, j)) {
                    return;
                }
            }
            for (int j = limit + 1; j <= seg.capacity; j++) {
                if (insertEntryInternalImpl(entry, key, seg, j)) {
                    return;
                }
            }
            // If no free slot was found in A1, trigger a resize since the table is full.
            resize();
            insertEntryInternal(entry);
        } else {
            int b = currentBatch;
            Segment<K, V> segB = segments[b];
            Segment<K, V> segBplus = (b + 1 < segments.length) ? segments[b + 1] : null;
            double epsilonB = segB.freeFraction();
            double epsilonBplus = (segBplus != null) ? segBplus.freeFraction() : 1.0;
            if (epsilonB > delta / 2.0 && epsilonBplus > 0.25 && segBplus != null) {
                int limit = probeLimit(epsilonB);
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
                for (int j = 1; j <= segB.capacity; j++) {
                    if (insertEntryInternalImpl(entry, key, b, segB, j)) {
                        return;
                    }
                }
                throw new IllegalStateException("Insertion failed in Segment " + b);
            } else {
                for (int j = 1; j <= segB.capacity; j++) {
                    if (insertEntryInternalImpl(entry, key, b, segB, j)) {
                        return;
                    }
                }
                throw new IllegalStateException("Insertion failed in Segment " + b);
            }
        }
    }

    /**
     * Attempts to insert an entry into a segment using linear probing.
     *
     * @param entry the entry to insert
     * @param key   the key to insert
     * @param b     the current batch index
     * @param seg   the segment to insert into
     * @return true if insertion succeeds, false otherwise
     */
    private boolean insertEntryInternalImpl(Entry<K, V> entry, K key, int b, Segment<K, V> seg) {
        for (int j = 1; j <= seg.capacity; j++) {
            if (insertEntryInternalImpl(entry, key, b, seg, j)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Attempts to insert an entry into a segment with a specified probe count.
     *
     * @param entry the entry to insert
     * @param key   the key to insert
     * @param b     the current batch index
     * @param seg   the segment to insert into
     * @param j     the probe count
     * @return true if insertion succeeds, false otherwise
     */
    private boolean insertEntryInternalImpl(Entry<K, V> entry, K key, int b, Segment<K, V> seg, int j) {
        int idx = probeIndex(seg, b, j, key);
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
     * Attempts to insert an entry in batch 0 into the segment.
     *
     * @param entry the entry to insert
     * @param key   the key to insert
     * @param seg   the segment to insert into
     * @param j     the probe count
     * @return true if insertion succeeds, false otherwise
     */
    private boolean insertEntryInternalImpl(Entry<K, V> entry, K key, Segment<K, V> seg, int j) {
        int idx = probeIndex(seg, 0, j, key);
        if (seg.table[idx] == null) {
            entry.segmentIndex = 0;
            entry.probeCount = j;
            seg.insert(idx, entry);
            size++;
            // If Segment A1 reaches the initial fill ratio and there are additional segments, move to batch 1.
            if (seg.count >= Math.ceil(INITIAL_FILL_RATIO * seg.capacity) && segments.length > 1) {
                currentBatch = 1;
            }
            return true;
        }
        return false;
    }

    /**
     * Updates the current batch if the current segment has reached its target fill level.
     */
    private void updateBatchIfNeeded() {
        if (currentBatch == 0) {
            Segment<K, V> seg = segments[0];
            if (seg.count >= Math.ceil(INITIAL_FILL_RATIO * seg.capacity) && segments.length > 1) {
                currentBatch = 1;
            }
        } else {
            Segment<K, V> seg = segments[currentBatch];
            int target = seg.capacity - (int) Math.floor(delta * seg.capacity / 2.0);
            if (seg.count >= target && currentBatch + 1 < segments.length) {
                currentBatch++;
            }
        }
    }

    /**
     * Searches for the key in the table and updates its value if found.
     *
     * @param key      the key to search for
     * @param newValue the new value to update if the key is found
     * @return the old value if found, or null otherwise
     */
    private V internalGetAndUpdate(K key, V newValue) {
        int maxSeg = Math.min(segments.length, currentBatch + 2);
        for (int segIdx = 0; segIdx < maxSeg; segIdx++) {
            Segment<K, V> seg = segments[segIdx];
            for (int j = 1; j <= seg.capacity; j++) {
                int idx = probeIndex(seg, segIdx, j, key);
                Entry<K, V> e = seg.table[idx];
                if (e == null) {
                    break;
                }
                if (e.key.equals(key)) {
                    V oldVal = e.value;
                    e.value = newValue;
                    return oldVal;
                }
            }
        }
        return null;
    }

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old value is replaced.
     *
     * @param key   the key with which the specified value is to be associated
     * @param value the value to be associated with the key
     * @return the previous value associated with the key, or null if there was none
     */
    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key, "Key cannot be null");
        V old = internalGetAndUpdate(key, value);
        if (old != null) {
            return old;
        }
        if (size >= threshold) {
            resize();
        }
        Entry<K, V> newEntry = new Entry<>(key, value, -1, -1);
        insertEntryInternal(newEntry);
        return null;
    }

    // ---------------------- Resizing ----------------------

    /**
     * Resizes the table by doubling its capacity and rehashing all existing entries.
     */
    private void resize() {
        int newCapacity = totalCapacity * 2;
        Segment<K, V>[] newSegments = createSegments(newCapacity);
        Segment<K, V>[] oldSegments = segments;
        totalCapacity = newCapacity;
        maxSize = newCapacity - (int) Math.floor(delta * newCapacity);
        threshold = maxSize;
        // Reset batch for the new table.
        currentBatch = 0;
        int oldSize = size;
        size = 0;
        segments = newSegments;
        // Set rehash mode to force insertion into batch 0.
        rehashMode = true;
        // Rehash all entries from the old segments.
        for (Segment<K, V> seg : oldSegments) {
            for (int i = 0; i < seg.capacity; i++) {
                Entry<K, V> e = seg.table[i];
                if (e != null) {
                    Entry<K, V> newEntry = new Entry<>(e.key, e.value, -1, -1);
                    insertEntryInternal(newEntry);
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
     * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value associated with the key, or null if not found
     */
    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }
        int maxSeg = Math.min(segments.length, currentBatch + 2);
        for (int segIdx = 0; segIdx < maxSeg; segIdx++) {
            Segment<K, V> seg = segments[segIdx];
            for (int j = 1; j <= seg.capacity; j++) {
                int idx = probeIndex(seg, segIdx, j, (K) key);
                Entry<K, V> e = seg.table[idx];
                if (e == null) {
                    break;
                }
                if (e.key.equals(key)) {
                    return e.value;
                }
            }
        }
        return null;
    }

    /**
     * Removes the mapping for the specified key from this map if present.
     *
     * @param key the key whose mapping is to be removed
     * @return the previous value associated with the key, or null if there was none
     */
    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }
        int maxSeg = Math.min(segments.length, currentBatch + 2);
        for (int segIdx = 0; segIdx < maxSeg; segIdx++) {
            Segment<K, V> seg = segments[segIdx];
            for (int j = 1; j <= seg.capacity; j++) {
                int idx = probeIndex(seg, segIdx, j, (K) key);
                Entry<K, V> e = seg.table[idx];
                if (e == null) {
                    break;
                }
                if (e.key.equals(key)) {
                    V oldVal = e.value;
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
     * @param key the key to test for presence
     * @return true if the key is present, false otherwise
     */
    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    /**
     * Returns true if this map maps one or more keys to the specified value.
     *
     * @param value the value whose presence is to be tested
     * @return true if the value is present in the map, false otherwise
     */
    @Override
    public boolean containsValue(Object value) {
        for (Segment<K, V> seg : segments) {
            for (Entry<K, V> e : seg.table) {
                if (e != null && Objects.equals(e.value, value)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Copies all the mappings from the specified map to this map.
     *
     * @param m mappings to be stored in this map
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    /**
     * Removes all the mappings from this map.
     */
    @Override
    public void clear() {
        for (Segment<K, V> seg : segments) {
            Arrays.fill(seg.table, null);
            seg.count = 0;
        }
        size = 0;
        currentBatch = 0;
    }

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the size of the map
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * Returns true if this map contains no key-value mappings.
     *
     * @return true if the map is empty
     */
    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    // ---------------------- View Collections ----------------------

    private transient Set<Map.Entry<K, V>> entrySet;

    /**
     * Returns a Set view of the mappings contained in this map.
     *
     * @return a set view of the map entries
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

                        private Entry<K, V> advance() {
                            while (segIdx < segments.length) {
                                Segment<K, V> seg = segments[segIdx];
                                while (pos < seg.capacity) {
                                    Entry<K, V> e = seg.table[pos++];
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
                            Map.Entry<K, V> ret = new AbstractMap.SimpleImmutableEntry<>(nextEntry.key, nextEntry.value);
                            nextEntry = advance();
                            return ret;
                        }
                    };
                }

                @Override
                public int size() {
                    return ElasticHashMap.this.size();
                }
            };
        }
        return entrySet;
    }

    /**
     * Returns a Set view of the keys contained in this map.
     *
     * @return a set of keys
     */
    @Override
    public Set<K> keySet() {
        Set<K> ks = new HashSet<>();
        for (Map.Entry<K, V> e : entrySet()) {
            ks.add(e.getKey());
        }
        return ks;
    }

    /**
     * Returns a Collection view of the values contained in this map.
     *
     * @return a collection of values
     */
    @Override
    public Collection<V> values() {
        List<V> vals = new ArrayList<>();
        for (Map.Entry<K, V> e : entrySet()) {
            vals.add(e.getValue());
        }
        return vals;
    }

    /**
     * Compares the specified object with this map for equality.
     *
     * @param o the object to be compared for equality with this map
     * @return true if the specified object is equal to this map, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Map<?, ?> m)) {
            return false;
        }
        if (m.size() != size()) {
            return false;
        }
        try {
            for (Map.Entry<K, V> e : entrySet()) {
                K key = e.getKey();
                V value = e.getValue();
                if (!Objects.equals(value, m.get(key))) {
                    return false;
                }
            }
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }
        return true;
    }

    /**
     * Returns the hash code value for this map.
     *
     * @return the hash code value for this map
     */
    @Override
    public int hashCode() {
        int h = 0;
        for (Map.Entry<K, V> e : entrySet()) {
            h += e.hashCode();
        }
        return h;
    }

    /**
     * Returns a string representation of this map.
     *
     * @return a string representation of the map
     */
    @Override
    public String toString() {
        Iterator<Map.Entry<K, V>> i = entrySet().iterator();
        if (!i.hasNext()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        while (true) {
            Map.Entry<K, V> e = i.next();
            sb.append(e.getKey()).append('=').append(e.getValue());
            if (!i.hasNext()) {
                return sb.append('}').toString();
            }
            sb.append(", ");
        }
    }
}
