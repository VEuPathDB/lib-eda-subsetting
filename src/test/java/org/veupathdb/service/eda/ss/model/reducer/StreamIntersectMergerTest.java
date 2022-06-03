package org.veupathdb.service.eda.ss.model.reducer;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Nested
public class StreamIntersectMergerTest {

  @Nested
  public class WhenInputIsSorted {
    private List<Integer> oneToOneHundred = IntStream.rangeClosed(1, 100)
        .mapToObj(Integer::valueOf)
        .collect(Collectors.toList());
    private List<Integer> evenNumbers = IntStream.rangeClosed(1, 100)
        .filter(i -> i % 2 == 0)
        .mapToObj(Integer::valueOf)
        .collect(Collectors.toList());
    private List<Integer> oddNumbers = IntStream.rangeClosed(1, 100)
        .filter(i -> i % 2 != 0)
        .mapToObj(Integer::valueOf)
        .collect(Collectors.toList());
    private List<Integer> threeFactors = IntStream.rangeClosed(1, 100)
        .filter(i -> i % 3 == 0)
        .mapToObj(Integer::valueOf)
        .collect(Collectors.toList());
    private List<Integer> fiveFactors = IntStream.rangeClosed(1, 100)
        .filter(i -> i % 5 == 0)
        .mapToObj(Integer::valueOf)
        .collect(Collectors.toList());

    @Test
    public void testTwoEqualStreams() {
      final Iterator<Integer> stream1 = new ArrayList(oneToOneHundred).iterator();
      final Iterator<Integer> stream2 = new ArrayList(oneToOneHundred).iterator();
      StreamIntersectMerger<Integer> merger = new StreamIntersectMerger<>(List.of(stream1, stream2), Comparator.naturalOrder());
      Iterable<Integer> iterable = () -> merger;
      MatcherAssert.assertThat(iterable, Matchers.iterableWithSize(100));
    }

    @Test
    public void testTwoDistinctStreams() {
      final Iterator<Integer> stream1 = new ArrayList(evenNumbers).iterator();
      final Iterator<Integer> stream2 = new ArrayList(oddNumbers).iterator();
      StreamIntersectMerger<Integer> merger = new StreamIntersectMerger<>(List.of(stream1, stream2), Comparator.naturalOrder());
      Iterable<Integer> result = () -> merger;
      MatcherAssert.assertThat(result, Matchers.emptyIterable());
    }

    @Test
    public void testStreamsSomeOverlap() {
      final Iterator<Integer> stream1 = new ArrayList(evenNumbers).iterator();
      final Iterator<Integer> stream2 = new ArrayList(threeFactors).iterator();
      final Iterator<Integer> stream3 = new ArrayList(fiveFactors).iterator();
      StreamIntersectMerger<Integer> merger = new StreamIntersectMerger<>(List.of(stream1, stream2, stream3),
          Comparator.naturalOrder());
      Iterable<Integer> result = () -> merger;
      MatcherAssert.assertThat(result, Matchers.contains(30, 60, 90));
    }
  }

  // TODO: Tests that randomly generate sets, sort and stream them through our merger and then validate that intersected
  // sets are equal to our output stream.
  public class WhenInputIsSortedAndRandom {

  }

  // TODO: This should probably throw an exception
  @Nested
  public class WhenInputIsNotSorted {

  }
}