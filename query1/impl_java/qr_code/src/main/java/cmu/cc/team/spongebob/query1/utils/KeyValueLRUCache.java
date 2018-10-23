package cmu.cc.team.spongebob.query1.utils;

import java.util.PriorityQueue;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;


public class KeyValueLRUCache {
    private static final int MAX_ITEM_NUMBER = 200000;

    private final ConcurrentHashMap<String, String> keyValueStore;
    private final ConcurrentHashMap<Long, String> keyTimestamp;
    private final PriorityQueue<Long> timestamps;

    private final long maxItemNumber;

    private static KeyValueLRUCache keyValueLRUCache;
    private static final Object keyValueStoreLock = new Object();

    public static KeyValueLRUCache getInstance() {
        synchronized (keyValueStoreLock) {
            if (keyValueLRUCache == null) {
                keyValueLRUCache = new KeyValueLRUCache(MAX_ITEM_NUMBER);
            }
        }
        return keyValueLRUCache;
    }

    private KeyValueLRUCache(long maxItemNumber) {
        keyValueStore = new ConcurrentHashMap<>();
        keyTimestamp = new ConcurrentHashMap<>();
        timestamps = new PriorityQueue<>();
        this.maxItemNumber = maxItemNumber;
    }

    public void put(String key, String value) {
        long timestamp = System.currentTimeMillis() + TimeZone.getTimeZone("EST").getRawOffset();

        synchronized (keyValueStore) {
            if (timestamps.size() > maxItemNumber) {
                long leastRecentTimestamp = timestamps.poll();
                String keyToDelete = keyTimestamp.get(leastRecentTimestamp);
                keyValueStore.remove(keyToDelete);
            }

            keyValueStore.put(key, value);
            keyTimestamp.put(timestamp, key);
            timestamps.offer(timestamp);
        }
    }

    public String get(String key) {
        return keyValueStore.get(key);
    }

    public void reset() {
        synchronized (keyValueStore) {
            keyValueStore.clear();
            keyTimestamp.clear();
            timestamps.clear();
        }
    }
}
