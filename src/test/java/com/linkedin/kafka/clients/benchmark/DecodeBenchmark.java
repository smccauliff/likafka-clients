package com.linkedin.kafka.clients.benchmark;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

import static com.linkedin.kafka.clients.benchmark.MapBenchmark.keys;
import static com.linkedin.kafka.clients.benchmark.MapBenchmark.measureMap;

public class DecodeBenchmark {

  private static void appendToByteBuffer(String key, ByteBuffer bbuf) {
    bbuf.put((byte) key.length());
    bbuf.put(key.getBytes());
    bbuf.putInt(0); //value length
  }

  private static void appendToByteBuffer(int key, ByteBuffer bbuf) {
    bbuf.putInt(key);
    bbuf.putInt(0); // value length
  }

  private static final ByteBuffer[] serializedStringHeaders = new ByteBuffer[keys.length];
  private static final ByteBuffer[] serializedIntHeaders = new ByteBuffer[keys.length];
  private static int roundRobinKeyIndex = 0;
  private static int roundRobinPermutations = 0;
  private static final int TEST_COUNT = 1_000_000;
  private static final int PERMUTATION_COUNT = 31;
  private static final int[][] permutations = new int[PERMUTATION_COUNT][];
  private static final Random random = new Random();
  private static final Charset utf8Charset = Charset.forName("UTF8");
  private static final String[] keysAsString = Arrays.stream(keys).map(a -> new String(a)).toArray(String[]::new);

  private static void fisherYeatesShuffle(int[] a) {
    for (int i=0; i < a.length; i++) {
      int destIndex = random.nextInt(a.length);
      int tmp = a[destIndex];
      a[destIndex] = a[i];
      a[i] = tmp;
    }
  }

  private static int key(int keyIndex, int bbufIndex) {
    return keyIndex * bbufIndex * 100;
  }

  static {
    int keyLengths = Arrays.stream(keys).mapToInt(c -> c.length).sum();
    for (int bbufIndex = 0; bbufIndex < keys.length; bbufIndex++) {
      serializedStringHeaders[bbufIndex] = ByteBuffer.allocate( (1 + 4) * keys.length + keyLengths);
      serializedIntHeaders[bbufIndex] = ByteBuffer.allocate( (4 + 4) * keys.length);
      for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
        appendToByteBuffer(new String(keys[ (keyIndex + bbufIndex) % keys.length]),  serializedStringHeaders[bbufIndex]);
        appendToByteBuffer(key(keyIndex, bbufIndex), serializedIntHeaders[bbufIndex]);
      }
    }

    for (int permutationIndex = 0; permutationIndex < PERMUTATION_COUNT; permutationIndex++) {
      permutations[permutationIndex] = new int[keys.length];
      for (int keyIndex=0; keyIndex < keys.length; keyIndex++) {
        permutations[permutationIndex][keyIndex] = keyIndex;
      }
      fisherYeatesShuffle(permutations[permutationIndex]);
    }
  }

  private static Map<String, byte[]> parseStringHeader(ByteBuffer bbuf, Map<String, byte[]> m, int parseLimit) {
    int headerCount = 0;
    while (bbuf.hasRemaining() && headerCount < parseLimit) {
      byte keyLength = bbuf.get();
      int oldLimit = bbuf.limit();
      bbuf.limit(bbuf.position() + keyLength);
      String key = utf8Charset.decode(bbuf).toString();
      key.intern();
      bbuf.limit(oldLimit);
      int valueLength = bbuf.getInt();
      byte[] value = new byte[valueLength];
      bbuf.get(value);
      m.put(key, value);
      headerCount++;
    }
    return m;
  }

  private static Map<Integer, byte[]> parseIntHeader(ByteBuffer bbuf, Map<Integer, byte[]> m, int parseLimit) {
    int headerCount = 0;
    while (bbuf.hasRemaining() && headerCount < parseLimit) {
      int key = bbuf.getInt();
      int valueLength = bbuf.getInt();
      byte[] value = new byte[valueLength];
      bbuf.get(value);
      m.put(key, value);
      headerCount++;
    }
    return m;
  }

  public static void main(String[] argv) throws Exception {
    System.out.println("Warmup.");
    for (int i = 0; i < 100_000; i++) {
      decodeWithString(12, TreeMap::new);
      decodeWithInt(12, TreeMap::new);
      decodeWithInt(12, HashMap::new);
      decodeWithString(12, HashMap::new);
      decodeWithInt(12, ListMap::new);
      decodeWithString(12, ListMap::new);
    }

    System.out.println("Integer-TreeMap");
    measureMap(itemCount -> decodeWithInt(itemCount, TreeMap::new));
    System.out.println("Integer-HashMap");
    measureMap(itemCount -> decodeWithInt(itemCount, HashMap::new));
    System.out.println("Integer-List");
    measureMap(itemCount -> decodeWithInt(itemCount, ListMap::new));
    System.out.println("String-TreeMap");
    measureMap(itemCount -> decodeWithString(itemCount, TreeMap::new));
    System.out.println("String-HashMap");
    measureMap(itemCount -> decodeWithString(itemCount, HashMap::new));
    System.out.println("String-List");
    measureMap(itemCount -> decodeWithString(itemCount, ListMap::new));

  }

  private static Object decodeWithInt(int itemCount, Supplier<Map<Integer, byte[]>> mapFactory) {
    Map<Integer, byte[]> m = mapFactory.get();
    int bbufIndex = roundRobinKeyIndex++ % keys.length;
    ByteBuffer serializedHeader = serializedIntHeaders[bbufIndex];
    serializedHeader.position(0);
    serializedHeader.limit(serializedHeader.capacity());
    parseIntHeader(serializedHeader, m, itemCount);
    int[] indicesToCheck  = permutations[roundRobinPermutations++ % PERMUTATION_COUNT];
    for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
      int permutedKey = indicesToCheck[keyIndex];
      boolean foundSomething = false;
      foundSomething = foundSomething || m.get(key(permutedKey, bbufIndex)) != null;
    }
    return null;
  }

  private static Object decodeWithString(int itemCount, Supplier<Map<String, byte[]>> mapFactory) {
    Map<String, byte[]> m = mapFactory.get();
    ByteBuffer serializedHeader = serializedStringHeaders[roundRobinKeyIndex++ % keys.length];
    serializedHeader.position(0);
    serializedHeader.limit(serializedHeader.capacity());
    parseStringHeader(serializedHeader, m, itemCount);
    int[] permutedKeyIndices = permutations[roundRobinPermutations++ % PERMUTATION_COUNT];
    boolean foundSomething = false;
    for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
      int permutedKey = permutedKeyIndices[keyIndex];
      foundSomething = foundSomething || m.get(keysAsString[permutedKey]) != null;
    }
    if (!foundSomething) {
      throw new IllegalStateException("Should have found something in map.");
    }
    return null;
  }

  /**
   * This class is a real hack to make testing easier. Please don't use this for anything real... ever.
   * @param <K>
   * @param <V>
   */
  private static final class ListMap<K, V> implements Map<K, V> {

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
}
