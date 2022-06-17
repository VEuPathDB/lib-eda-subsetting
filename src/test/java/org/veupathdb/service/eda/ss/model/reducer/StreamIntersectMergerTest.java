package org.veupathdb.service.eda.ss.model.reducer;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

@Nested
public class StreamIntersectMergerTest {

  @Nested
  public class WhenInputIsSorted {
    private final List<Long> oneToOneHundred = LongStream.rangeClosed(1, 100)
        .boxed()
        .collect(Collectors.toList());
    private final List<Long> evenNumbers = LongStream.rangeClosed(1, 100)
        .filter(i -> i % 2 == 0)
        .boxed()
        .collect(Collectors.toList());
    private final List<Long> oddNumbers = LongStream.rangeClosed(1, 100)
        .filter(i -> i % 2 != 0)
        .boxed()
        .collect(Collectors.toList());
    private final List<Long> threeFactors = LongStream.rangeClosed(1, 100)
        .filter(i -> i % 3 == 0)
        .boxed()
        .collect(Collectors.toList());
    private final List<Long> fiveFactors = LongStream.rangeClosed(1, 100)
        .filter(i -> i % 5 == 0)
        .boxed()
        .collect(Collectors.toList());

    @Test
    public void testTwoEqualStreams() {
      final Iterator<Long> stream1 = new ArrayList<>(oneToOneHundred).iterator();
      final Iterator<Long> stream2 = new ArrayList<>(oneToOneHundred).iterator();
      StreamIntersectMerger merger = new StreamIntersectMerger(List.of(stream1, stream2));
      Iterable<Long> iterable = () -> merger;
      MatcherAssert.assertThat(iterable, Matchers.iterableWithSize(100));
    }

    @Test
    public void testTwoDistinctStreams() {
      final Iterator<Long> stream1 = new ArrayList<>(evenNumbers).iterator();
      final Iterator<Long> stream2 = new ArrayList<>(oddNumbers).iterator();
      StreamIntersectMerger merger = new StreamIntersectMerger(List.of(stream1, stream2));
      Iterable<Long> result = () -> merger;
      MatcherAssert.assertThat(result, Matchers.emptyIterable());
    }

    @Test
    public void testStreamsSomeOverlap() {
      final Iterator<Long> stream1 = new ArrayList<>(evenNumbers).iterator();
      final Iterator<Long> stream2 = new ArrayList<>(threeFactors).iterator();
      final Iterator<Long> stream3 = new ArrayList<>(fiveFactors).iterator();
      StreamIntersectMerger merger = new StreamIntersectMerger(List.of(stream1, stream2, stream3));
      Iterable<Long> result = () -> merger;
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
