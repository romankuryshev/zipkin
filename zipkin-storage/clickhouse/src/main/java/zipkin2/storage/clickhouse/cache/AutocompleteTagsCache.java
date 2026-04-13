package zipkin2.storage.clickhouse.cache;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class AutocompleteTagsCache {

  private static final class CacheEntry {
    private final Set<String> values;
    private long createdTime;

    CacheEntry() {
      this.values = new HashSet<>();
      this.createdTime = System.currentTimeMillis();
    }

    boolean isExpired(long ttlMillis) {
      return (System.currentTimeMillis() - createdTime) > ttlMillis;
    }
  }

  private final Map<String, CacheEntry> cache;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final int ttlMillis;
  private final int maxCardinality;
  private final Set<String> configuredKeys;

  public AutocompleteTagsCache(int ttlMillis, int maxCardinality, Set<String> configuredKeys) {
    this.cache = new ConcurrentHashMap<>();
    this.ttlMillis = ttlMillis;
    this.maxCardinality = maxCardinality;
    this.configuredKeys = configuredKeys;
  }

  public void put(String tagKey, Collection<String> values) {
    if (tagKey == null || tagKey.isEmpty() || values == null || values.isEmpty()) {
      return;
    }

    // Only cache configured keys
    if (!configuredKeys.contains(tagKey)) {
      return;
    }

    lock.writeLock().lock();
    try {
      CacheEntry entry = cache.computeIfAbsent(tagKey, k -> new CacheEntry());

      // Add new values, respecting cardinality limit
      for (String value : values) {
        if (value != null && !value.isEmpty() && entry.values.size() < maxCardinality) {
          entry.values.add(value);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Set<String> get(String tagKey) {
    if (tagKey == null || tagKey.isEmpty()) {
      return Set.of();
    }

    lock.readLock().lock();
    try {
      CacheEntry entry = cache.get(tagKey);
      if (entry == null || entry.isExpired(ttlMillis)) {
        return Set.of();
      }
      return Set.copyOf(entry.values);
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean contains(String tagKey) {
    if (tagKey == null || tagKey.isEmpty()) {
      return false;
    }

    lock.readLock().lock();
    try {
      CacheEntry entry = cache.get(tagKey);
      return entry != null && !entry.isExpired(ttlMillis) && !entry.values.isEmpty();
    } finally {
      lock.readLock().unlock();
    }
  }
}

