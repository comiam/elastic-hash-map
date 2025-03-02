package comiam;


public class Main {

    public static void main(String[] args) {
        ElasticHashMap<String, Integer> map = new ElasticHashMap<>(1024, 0.125);

        map.put("apple", 1);
        map.put("banana", 2);
        map.put("orange", 3);

        System.out.println("apple = " + map.get("apple"));
        System.out.println("banana = " + map.get("banana"));
        System.out.println("Size: " + map.size());

        map.put("apple", 10);
        System.out.println("Updated apple = " + map.get("apple"));

        map.remove("banana");
        System.out.println("After removal, banana = " + map.get("banana"));
        System.out.println("Size: " + map.size());

        for (int i = 0; i < 2000; i++) {
            map.put("key" + i, i);
        }
        System.out.println("After additional insertions, size: " + map.size());
        System.out.println("Map hash code: " + map.hashCode());
        System.out.println("Map toString: " + map);
    }
}