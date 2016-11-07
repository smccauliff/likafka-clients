package com.linkedin.kafka.clients.benchmark;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.util.MathArrays;


/**
 *
 */
public class StringKeyLengthBenchmark {


  private static final MersenneTwister RANDOM = new MersenneTwister(948676238753L);
  private static final char KEY_CHAR_START = 'a';
  private static int nextCharKeyIndex = 0;
  //Can only have as many keys as different characters since the first n-1 chars can be the same
  private static final int HEADER_COUNT = 20;
  private static final int KEY_COUNT = HEADER_COUNT;
  private static final int PERMUTATION_COUNT = 71;
  private static final int TEST_COUNT = 1_000_000;
  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
  private static final RandomStringPermutationGenerator stringGenerator;

  static {
    Set<Character> characters = new HashSet<>();
    for ( char c = KEY_CHAR_START; c < (KEY_CHAR_START + KEY_COUNT); c++) {
      characters.add(c);
    }
    stringGenerator = new RandomStringPermutationGenerator(characters, RANDOM);
  }

  private static final class TestCase {
    private final String[] keys;
    private final ByteBuffer[] serializedHeaders;
    private final int[][] keyLookupPermutations;

    public TestCase(String[] keys, ByteBuffer[] serializedHeaders, int[][] keyLookupPermutations) {
      this.keys = keys;
      this.serializedHeaders = serializedHeaders;
      this.keyLookupPermutations = keyLookupPermutations;
    }
  }

  private static TestCase testCase(int stringPrefixLength, int totalStringLength) {
    if (stringPrefixLength >= totalStringLength) {
      throw new IllegalStateException("Invalid prefix/length combination");
    }

    String prefix = (stringPrefixLength > 0) ? stringGenerator.permutations(1, stringPrefixLength).iterator().next() : "";

    //System.out.println("Generating keys.");
    Set<String> suffixes = stringGenerator.permutations(HEADER_COUNT, totalStringLength - stringPrefixLength);
    String[] keys = suffixes.stream().map(s -> prefix + s).toArray(String[]::new);
    //Arrays.stream(keys).forEach(s -> System.out.println(s));

    int[][] permutations = new int[PERMUTATION_COUNT][];
    for (int permutationIndex = 0; permutationIndex < PERMUTATION_COUNT; permutationIndex++) {
      int[] permutation = new int[HEADER_COUNT];
      for (int i=0; i < HEADER_COUNT; i++) {
        permutation[i] = i;
      }
      MathArrays.shuffle(permutation, RANDOM);
      permutations[permutationIndex] = permutation;
    }

    ByteBuffer[] serializedKeys = new ByteBuffer[HEADER_COUNT * HEADER_COUNT];
    for (int serializedKeyIndex = 0; serializedKeyIndex < serializedKeys.length; serializedKeyIndex++) {
      serializedKeys[serializedKeyIndex] = ByteBuffer.allocate((totalStringLength + 1 + 4) * HEADER_COUNT);
      for (int keyIndex = 0; keyIndex < HEADER_COUNT; keyIndex++) {
        serializedKeys[serializedKeyIndex].put((byte) totalStringLength);
        serializedKeys[serializedKeyIndex].put(keys[ (keyIndex + serializedKeyIndex) % HEADER_COUNT].getBytes());
        serializedKeys[serializedKeyIndex].putInt(0);
      }
      if (serializedKeys[serializedKeyIndex].limit() != serializedKeys[serializedKeyIndex].capacity()) {
        throw new IllegalStateException("Didn't fill serialized buffer.");
      }
    }

    return new TestCase(keys, serializedKeys, permutations);
  }

  public static void main(String[] argv) throws Exception {
    System.out.println("Warmup");
    runTestCase(1, 2);
    System.out.println("Warmup complete.");
    for (int keyLength=1; keyLength < 128; keyLength++) {
      int keyPrefixLength = (int) (keyLength * 0.1);
      runTestCase(keyPrefixLength, keyLength);
    }
  }

  private static void runTestCase(int keyPrefixLength, int keyLength) {
    TestCase testCase = testCase(keyPrefixLength, keyLength);
    long startTime = System.currentTimeMillis();
    int serializedHeaderIndex = 0;
    int permutationIndex = 0;
    for (int testIteration = 0; testIteration < TEST_COUNT; testIteration++) {
      Map<String, byte[]> headerMap = new ListMap<>();
      ByteBuffer serializedHeader = testCase.serializedHeaders[serializedHeaderIndex++ % HEADER_COUNT];
      serializedHeader.position(0);
      serializedHeader.limit(serializedHeader.capacity());
      parseStringHeader(serializedHeader, headerMap);
      if (headerMap.size() != HEADER_COUNT) {
        throw new IllegalStateException();
      }

      int[] permutation = testCase.keyLookupPermutations[permutationIndex++ % PERMUTATION_COUNT];
      for (int searchIndex = 0; searchIndex < HEADER_COUNT; searchIndex++) {
        byte[] value = headerMap.get(testCase.keys[permutation[searchIndex]]);
        if (value == null) {
          throw new IllegalStateException("Missing header key.");
        }
      }
    }
    long endTime = System.currentTimeMillis();
    double durationMs = endTime - startTime;
    double millisPerHeaderSet = durationMs / TEST_COUNT;
    System.out.println(keyPrefixLength + "," + keyLength + "," + millisPerHeaderSet);
  }

  private static void parseStringHeader(ByteBuffer bbuf, Map<String, byte[]> m) {
    while (bbuf.hasRemaining()) {
      byte keyLength = bbuf.get();
      int oldLimit = bbuf.limit();
      bbuf.limit(bbuf.position() + keyLength);
      String key = UTF8_CHARSET.decode(bbuf).toString();
      //key.intern();
      bbuf.limit(oldLimit);
      int valueLength = bbuf.getInt();
      byte[] value = new byte[valueLength];
      bbuf.get(value);
      m.put(key, value);
    }
  }
}
