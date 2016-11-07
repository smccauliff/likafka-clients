package com.linkedin.kafka.clients.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.math3.random.RandomAdaptor;
import org.apache.commons.math3.random.RandomGenerator;


public class RandomStringPermutationGenerator {

  private final List<Character> originalSet;
  private final RandomGenerator random;

  public RandomStringPermutationGenerator(Set<Character> originalSet, RandomGenerator random) {
    this.originalSet = new ArrayList<>(originalSet);
    this.random = random;
  }

  /**
   * TODO:  This is broken in that it picks from a much smaller key space
   *
   * @param count the number of permutations to generate
   * @param length the length of the strings to generate
   * @return A set of randomly generated strings of size count each of which has a length of length.
   */
  public Set<String> permutations(int count, int length) {
    if (count > length * originalSet.size()) {
      throw new IllegalArgumentException("Can't generate that many strings given the number string length and number of characters.");
    }
    List<Deque<Character>> remainingCharacters = new ArrayList<>(length);
    RandomAdaptor rand = new RandomAdaptor(random);
    for (int i = 0; i < length; i++) {
      Collections.shuffle(originalSet, rand);
      Deque<Character> shuffledCharacters = new LinkedList<>(originalSet);
      remainingCharacters.add(shuffledCharacters);
    }

    Set<String> generatedStrings = new TreeSet<>();
    for (int stringIndex = 0; stringIndex < count; stringIndex++) {
      StringBuilder bldr = new StringBuilder(length);
      for (int charIndex = 0; charIndex < length; charIndex++) {
        char c = remainingCharacters.get(charIndex).pop();
        bldr.append(c);
      }
      generatedStrings.add(bldr.toString());
    }
    if (generatedStrings.size() != count) {
      throw new IllegalStateException("Failed to generate the expected number of strings.");
    }
    return generatedStrings;
  }
}
