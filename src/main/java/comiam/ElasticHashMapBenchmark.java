package comiam;


import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Benchmark comparing java.util.HashMap and ElasticHashMap.
 *
 * <p>Benchmarks:
 * <ul>
 *   <li>benchmarkHashMapPut: Insertion of mapSize keys into a standard HashMap.
 *   <li>benchmarkElasticHashMapPut: Insertion of mapSize keys into ElasticHashMap.
 *   <li>benchmarkHashMapGet: Lookup of keys in a standard HashMap.
 *   <li>benchmarkElasticHashMapGet: Lookup of keys in ElasticHashMap.
 * </ul>
 * </p>
 *
 * <p>Both maps are initialized with an initial capacity roughly twice the mapSize. The ElasticHashMap uses a delta value of 0.125.
 * The expected complexities are:
 * <ul>
 *   <li>Insertion:
 *       <ul>
 *         <li>Best-case expected: O(1) per insertion.
 *         <li>Worst-case expected: O(log(1/delta)) per insertion.
 *       </ul>
 *   <li>Lookup:
 *       <ul>
 *         <li>Best-case: O(1).
 *         <li>Worst-case expected: O(log(1/delta)).
 *       </ul>
 * </ul>
 * Resizing (when necessary) takes O(n) time but is amortized over all insertions.
 * </p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class ElasticHashMapBenchmark { //

    @Param({"1024", "4096", "16384"})
    public int mapSize;

    public List<String> keys;

    @Setup(Level.Trial)
    public void setup() {
        keys = new ArrayList<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            keys.add("key" + i);
        }
        Collections.shuffle(keys);
    }

    /**
     * Benchmark for insertion (put) in a standard HashMap.
     */
    @Benchmark
    public Map<String, Integer> benchmarkHashMapPut() {
        Map<String, Integer> map = new HashMap<>(mapSize * 2);
        for (int i = 0; i < keys.size(); i++) {
            map.put(keys.get(i), i);
        }
        return map;
    }

    /**
     * Benchmark for insertion (put) in ElasticHashMap.
     */
    @Benchmark
    public Map<String, Integer> benchmarkElasticHashMapPut() {
        Map<String, Integer> map = new ElasticHashMap<>(mapSize * 2, 0.125);
        for (int i = 0; i < keys.size(); i++) {
            map.put(keys.get(i), i);
        }
        return map;
    }

    /**
     * Benchmark for lookup (get) in a standard HashMap.
     */
    @Benchmark
    public int benchmarkHashMapGet() {
        Map<String, Integer> map = new HashMap<>(mapSize * 2);
        for (int i = 0; i < keys.size(); i++) {
            map.put(keys.get(i), i);
        }
        int sum = 0;
        for (String key : keys) {
            sum += map.get(key);
        }
        return sum;
    }

    /**
     * Benchmark for lookup (get) in ElasticHashMap.
     */
    @Benchmark
    public int benchmarkElasticHashMapGet() {
        Map<String, Integer> map = new ElasticHashMap<>(mapSize * 2, 0.125);
        for (int i = 0; i < keys.size(); i++) {
            map.put(keys.get(i), i);
        }
        int sum = 0;
        for (String key : keys) {
            sum += map.get(key);
        }
        return sum;
    }
}