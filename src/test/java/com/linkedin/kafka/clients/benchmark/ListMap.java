package com.linkedin.kafka.clients.benchmark;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * This class is a real hack to make testing easier. Please don't use this for anything real... ever.
 * @param <K>
 * @param <V>
 */
final class ListMap<K, V> implements Map<K, V> {

  private final List<MapBenchmark.Header<K, V>> backingList = new ArrayList<>();

  public ListMap() {

  }

  @Override
  public int size() {
    //This does not check for duplicates
    return backingList.size();
  }

  @Override
  public boolean isEmpty() {
    return backingList.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V get(Object key) {
    //searching backwards means we get the last value that was put for some key
    for (int i=backingList.size()-1; i >= 0; i--) {
      if (backingList.get(i).key.equals(key)) {
        return backingList.get(i).value;
      }
    }
    return null;
  }

  @Override
  public V put(K key, V value) {
    backingList.add(new MapBenchmark.Header<>(key, value));
    return null; //this breaks map spec
  }

  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<K> keySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<V> values() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    throw new UnsupportedOperationException();
  }
}
