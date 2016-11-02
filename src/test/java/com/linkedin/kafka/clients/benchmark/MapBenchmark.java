package com.linkedin.kafka.clients.benchmark;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.IntFunction;


public class MapBenchmark {
  private static final Random rand = new Random(4544544402L);
  private static final byte[] value = new byte[0];
  private static final int ITEM_COUNT_MAX = 20;
  private static final int TEST_COUNT_MAX = 1_000_000;
  static char[][] keys = new char[][] {
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

  private static int nextStringIndex = 0;

  public static void main(String[] argv) throws Exception {
    System.out.println("Warmup");
    for (int i=0; i < 100_000; i++) {
      createHashMap(10);
      createIntObjectHashMap(10);
      createISM(10);
      createISMPreSorted(10);
      createList(10);
      createHashMapString(ITEM_COUNT_MAX, 10);
      createTreeMap(10);
      createTreeMapString(10);
    }

    System.out.println("TreeMap");
    measureMap(MapBenchmark::createTreeMap);
    System.out.println("TreeMap String");
    measureMap(MapBenchmark::createTreeMapString);
    System.out.println("HashMap String preallocate 64");
    measureMap(count -> createHashMapString(count, 64));
    System.out.println("HashMap String preallocate 10");
    measureMap(count -> createHashMapString(count, 10));

    System.out.println("HashMap.");
    measureMap(MapBenchmark::createHashMap);
//    System.out.println("Trove IntObjectHashMap");
//    measureMap(MapBenchmark::createIntObjectHashMap);
//    System.out.println("Guava ImmutableSortedMap");
//    measureMap(MapBenchmark::createISM);
//    System.out.println("Guava ImmutableSortedMap presorted");
//    measureMap(MapBenchmark::createISMPreSorted);
    System.out.println("ArrayList");
    measureMap(MapBenchmark::createList);

  }

  static <T> void measureMap(IntFunction<T> doSomething) {
    for (int itemCount=1; itemCount <= ITEM_COUNT_MAX; itemCount++) {
      long startTime = System.currentTimeMillis();
      for (int i=0; i < TEST_COUNT_MAX; i++) {
        doSomething.apply(itemCount);
      }
      long endTime = System.currentTimeMillis();
      double elapsedTime = endTime - startTime;
      double millisPerMap = (elapsedTime / TEST_COUNT_MAX);
      System.out.println(itemCount + "," + millisPerMap);
    }
  }
  private static int key(int itemNumber) {
    return (itemNumber & 1) == 0 ? itemNumber  + 100000 : itemNumber;
  }

  private static Map<FakeString, byte[]> createHashMapString(int itemCount, int allocationSize) {
    Map<FakeString, byte[]> m = new HashMap<>(allocationSize);
    for (int i=0; i < itemCount; i++) {
      int keyIndex = nextStringIndex++ % keys.length;
      FakeString key = new FakeString(keys[keyIndex]);
      m.put(key, value);
    }
    return m;
  }

  private static TreeMap<FakeString, byte[]> createTreeMapString(int itemCount) {
    TreeMap<FakeString, byte[]> m = new TreeMap<>();
    for (int i=0; i < itemCount; i++) {
      int keyIndex = nextStringIndex++ % keys.length;
      FakeString key = new FakeString(keys[keyIndex]);
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

  private static TreeMap<Integer, byte[]> createTreeMap(int itemCount) {
    TreeMap<Integer, byte[]> m = new TreeMap<>();
    for (int i=0; i < itemCount; i++) {
      int key = key(i);
      m.put(key, value);
    }
    return m;
  }


  static final class Header<K, V> {
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
  private static final class FakeString implements Comparable<FakeString> {
    private int hashCode = 0;
    private final char[] data;

    FakeString(char[] data) {
      this.data = data;
    }

    FakeString(FakeString other) {
      this.data = other.data;
      this.hashCode = other.hashCode;
    }

    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof FakeString) {
        FakeString anotherString = (FakeString) anObject;
        int n = value.length;
        if (n == anotherString.data.length) {
          char v1[] = data;
          char v2[] = anotherString.data;
          int i = 0;
          while (n-- != 0) {
            if (v1[i] != v2[i]) return false;
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

    @Override
    public int compareTo(FakeString anotherString) {
      int len1 = data.length;
      int len2 = anotherString.data.length;
      int lim = Math.min(len1, len2);
      char v1[] = data;
      char v2[] = anotherString.data;

      int k = 0;
      while (k < lim) {
        char c1 = v1[k];
        char c2 = v2[k];
        if (c1 != c2) {
          return c1 - c2;
        }
        k++;
      }
      return len1 - len2;
    }
  }
}
