package com.linkedin.kafka.clients.benchmark;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.IntFunction;


public class MapBenchmark {
  private static final Random rand = new Random(4544544402L);
  private static final byte[] value = new byte[0];
  private static final int ITEM_COUNT = 20;
  private static final int MAP_COUNT = 1_000_000;
  private static char[][] keys = new char[][] {
    "linkedin.consumer.class".toCharArray(),
      "kafka.feature1".toCharArray(),
      "kafka.feature2".toCharArray(),
      "LinkedIn.group.id".toCharArray(),
      "LinkedIn.avro.schema.id".toCharArray(),
      "something.something.darkside.something.something.complete".toCharArray(),
      "cats.don't.care.about.kafka".toCharArray(),
      "argle bargle".toCharArray(),
      "org.apache.kafka.something.something.too.long".toCharArray(),
      "LinkedIn.priority".toCharArray(),
      "LinkedIn.one-more-key".toCharArray(),
      "header-11".toCharArray(),
      "header-12a".toCharArray(),
      "header-13aa".toCharArray(),
      "header-14".toCharArray(),
      "header-15".toCharArray(),
      "header-16aaa".toCharArray(),
      "header-17".toCharArray(),
      "header-18".toCharArray(),
      "header-19".toCharArray()
  };

  public static void main(String[] argv) throws Exception {
    System.out.println("Warmup");
    for (int i=0; i < 100_000; i++) {
      createHashMap(10);
      createIntObjectHashMap(10);
      createISM(10);
      createISMPreSorted(10);
      createList(10);
      createHashMapString(ITEM_COUNT);
    }

    System.out.println("HashMap String");
    measureMap(MapBenchmark::createHashMapString);
    System.out.println("HashMap.");
    measureMap(MapBenchmark::createHashMap);
    System.out.println("Trove IntObjectHashMap");
    measureMap(MapBenchmark::createIntObjectHashMap);
    System.out.println("Guava ImmutableSortedMap");
    measureMap(MapBenchmark::createISM);
    System.out.println("Guava ImmutableSortedMap presorted");
    measureMap(MapBenchmark::createISMPreSorted);
    System.out.println("ArrayList");
    measureMap(MapBenchmark::createList);

  }

  private static <T> void measureMap(IntFunction<T> createMap) {
    for (int itemCount=1; itemCount <= ITEM_COUNT; itemCount++) {
      long startTime = System.currentTimeMillis();
      for (int i=0; i < MAP_COUNT; i++) {
        createMap.apply(itemCount);
      }
      long endTime = System.currentTimeMillis();
      double elaspedTime = endTime - startTime;
      double millisPerMap = (elaspedTime / MAP_COUNT);
      System.out.println(itemCount + "," + millisPerMap);
    }
  }
  private static int key(int itemNumber) {
    return (itemNumber & 1) == 0 ? itemNumber  + 100000 : itemNumber;
  }

  private static Map<FakeString, byte[]> createHashMapString(int itemCount) {
    Map<FakeString, byte[]> m = new HashMap<>();
    for (int i=0; i < itemCount; i++) {
      FakeString key = new FakeString(keys[i]);
      m.put(key, value);
    }
    return m;
  }

  private static Map<Integer, byte[]> createHashMap(int itemCount) {
    Map<Integer, byte[]> m = new HashMap<>();
    for (int i=0; i < itemCount; i++) {
      int key = key(i);
      m.put(key, value);
    }
    return m;
  }

  private static TIntObjectHashMap createIntObjectHashMap(int itemCount) {
    TIntObjectHashMap<byte[]> m = new TIntObjectHashMap<>();
    for (int i=0; i < itemCount; i++) {
      int key = key = key(i);
      m.put(key, value);
    }
    return m;
  }

  private static ImmutableSortedMap<Integer, byte[]> createISM(int itemCount) {
    ImmutableSortedMap.Builder<Integer, byte[]> ism = new ImmutableSortedMap.Builder<>(Ordering.natural());
    for (int i=0; i < itemCount; i++) {
      int key = key = key(i);
      ism.put(key, value);
    }
    return ism.build();
  }

  private static ImmutableSortedMap<Integer, byte[]> createISMPreSorted(int itemCount) {
    ImmutableSortedMap.Builder<Integer, byte[]> ism = new ImmutableSortedMap.Builder<>(Ordering.natural());
    for (int i=0; i < itemCount; i++) {
      ism.put(i, value);
    }
    return ism.build();
  }

  private static List<Header<Integer, byte[]>> createList(int itemCount) {
    List<Header<Integer, byte[]>> a = new ArrayList<>();
    for (int i=0; i < itemCount; i++) {
      int key = key(i);
      a.add(new Header(i, value));
    }
    return a;
  }


  private static final class Header<K, V> {
    final K key;
    final V value;

    public Header(K key, V value) {
      this.key = key;
      this.value = value;
    }
  }

  /**
   * java.lang.String caches the results of hashCode and passes it along to new String(String).
   */
  private static final class FakeString {
    private int hashCode = 0;
    private final char[] data;

    FakeString(char[] data) {
      this.data = data;
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof FakeString) {
        FakeString anotherString = (FakeString)anObject;
        int n = value.length;
        if (n == anotherString.data.length) {
          char v1[] = data;
          char v2[] = anotherString.data;
          int i = 0;
          while (n-- != 0) {
            if (v1[i] != v2[i])
              return false;
            i++;
          }
          return true;
        }
      }
      return false;
    }

    @Override
    public int hashCode() {
      int h = hashCode;
      if (h == 0 && value.length > 0) {
        for (int i = 0; i < value.length; i++) {
          h = 31 * h + data[i];
        }
        hashCode = h;
      }
      return h;
    }
  }
}
