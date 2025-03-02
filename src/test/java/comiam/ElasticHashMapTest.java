package comiam;


import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests for the ElasticHashMap class.
 */
public class ElasticHashMapTest {

    /**
     * Tests basic insertion and retrieval.
     */
    @Test
    public void testPutAndGet() {
        ElasticHashMap<String, Integer> map = new ElasticHashMap<>(1024, 0.125);
        // Inserting a new key should return null.
        assertNull(map.put("apple", 1), "Inserting a new key should return null");
        assertEquals(1, map.get("apple"), "Value for key 'apple' should be 1");
        assertEquals(1, map.size(), "Size should be 1 after one insertion");
    }

    /**
     * Tests updating the value for an existing key.
     */
    @Test
    public void testPutUpdate() {
        ElasticHashMap<String, Integer> map = new ElasticHashMap<>(1024, 0.125);
        assertNull(map.put("apple", 1));
        // Update the value for key "apple" and check the returned old value.
        Integer oldValue = map.put("apple", 10);
        assertEquals(1, oldValue, "Old value should be 1 when updating key 'apple'");
        assertEquals(10, map.get("apple"), "New value for key 'apple' should be 10");
        assertEquals(1, map.size(), "Size should remain 1 after updating an existing key");
    }

    /**
     * Tests removal of a key.
     */
    @Test
    public void testRemove() {
        ElasticHashMap<String, Integer> map = new ElasticHashMap<>(1024, 0.125);
        map.put("banana", 2);
        assertEquals(2, map.get("banana"), "Value for 'banana' should be 2 before removal");
        Integer removedValue = map.remove("banana");
        assertEquals(2, removedValue, "Removed value should be 2");
        assertNull(map.get("banana"), "Value for 'banana' should be null after removal");
        assertEquals(0, map.size(), "Size should be 0 after removal");
    }

    /**
     * Tests the map resize by inserting a large number of keys.
     */
    @Test
    public void testResize() {
        ElasticHashMap<String, Integer> map = new ElasticHashMap<>(1024, 0.125);
        // Insert 2000 keys; the map should resize internally.
        for (int i = 0; i < 2000; i++) {
            map.put("key" + i, i);
        }
        assertEquals(2000, map.size(), "Size should be 2000 after inserting 2000 keys");
        // Verify that each key returns the correct value.
        for (int i = 0; i < 2000; i++) {
            assertEquals(i, map.get("key" + i), "Value for key" + i + " should be " + i);
        }
        // Check that the string representation is correctly formatted.
        String mapString = map.toString();
        assertTrue(mapString.startsWith("{") && mapString.endsWith("}"),
                "toString should start with '{' and end with '}'");
    }

    /**
     * Tests that inserting a null key throws a NullPointerException.
     */
    @Test
    public void testNullKeyInsertion() {
        ElasticHashMap<String, Integer> map = new ElasticHashMap<>(1024, 0.125);
        assertThrows(NullPointerException.class, () -> map.put(null, 1),
                "Inserting a null key should throw NullPointerException");
    }

    /**
     * Tests that invalid initial capacity or delta parameters throw IllegalArgumentException.
     */
    @Test
    public void testInvalidInitialCapacityAndDelta() {
        // Initial capacity must be positive.
        assertThrows(IllegalArgumentException.class, () -> new ElasticHashMap<>(0, 0.125),
                "Initial capacity 0 should throw IllegalArgumentException");
        // Delta must be in (0, 1).
        assertThrows(IllegalArgumentException.class, () -> new ElasticHashMap<>(1024, 1.0),
                "Delta equal to 1.0 should throw IllegalArgumentException");
        assertThrows(IllegalArgumentException.class, () -> new ElasticHashMap<>(1024, 0.0),
                "Delta equal to 0.0 should throw IllegalArgumentException");
    }

    /**
     * Tests the view collections: keySet, values, and entrySet.
     */
    @Test
    public void testViewCollections() {
        ElasticHashMap<String, Integer> map = new ElasticHashMap<>(1024, 0.125);
        map.put("apple", 1);
        map.put("banana", 2);
        map.put("orange", 3);

        Set<String> keys = map.keySet();
        Collection<Integer> values = map.values();
        Set<Map.Entry<String, Integer>> entries = map.entrySet();

        assertEquals(3, keys.size(), "Key set size should be 3");
        assertTrue(keys.contains("apple") && keys.contains("banana") && keys.contains("orange"),
                "Key set should contain all inserted keys");
        assertEquals(3, values.size(), "Values collection size should be 3");
        assertEquals(3, entries.size(), "Entry set size should be 3");
        // Verify that each entry's key and value are in the corresponding collections.
        for (Map.Entry<String, Integer> entry : entries) {
            assertTrue(keys.contains(entry.getKey()),
                    "Entry key should be in key set");
            assertTrue(values.contains(entry.getValue()),
                    "Entry value should be in values collection");
        }
    }

    /**
     * Tests clearing the map.
     */
    @Test
    public void testClear() {
        ElasticHashMap<String, Integer> map = new ElasticHashMap<>(1024, 0.125);
        map.put("apple", 1);
        map.put("banana", 2);
        map.put("orange", 3);
        assertEquals(3, map.size(), "Size should be 3 before clear");
        map.clear();
        assertEquals(0, map.size(), "Size should be 0 after clear");
        assertTrue(map.isEmpty(), "Map should be empty after clear");
        assertNull(map.get("apple"), "Map should not contain key 'apple' after clear");
    }

    /**
     * Tests the equals and hashCode methods.
     */
    @Test
    public void testEqualsAndHashCode() {
        ElasticHashMap<String, Integer> map1 = new ElasticHashMap<>(1024, 0.125);
        ElasticHashMap<String, Integer> map2 = new ElasticHashMap<>(1024, 0.125);

        map1.put("apple", 1);
        map1.put("banana", 2);
        map2.put("banana", 2);
        map2.put("apple", 1);

        assertEquals(map1, map2, "Maps with the same entries should be equal");
        assertEquals(map1.hashCode(), map2.hashCode(),
                "Hash codes should be equal for equal maps");
    }
}
