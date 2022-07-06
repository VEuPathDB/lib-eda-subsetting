package org.veupathdb.service.eda.ss.model.reducer;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;
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
    public void testOneStream() {
      final Iterator<Long> stream1 = new ArrayList<>(oneToOneHundred).iterator();
      StreamIntersectMerger merger = new StreamIntersectMerger(List.of(stream1));
      Iterable<Long> iterable = () -> merger;
      MatcherAssert.assertThat(iterable, Matchers.iterableWithSize(100));
    }

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
      final List<Long> stream1 = new ArrayList<>(evenNumbers);
      final List<Long> stream2 = new ArrayList<>(threeFactors);
      final List<Long> stream3 = new ArrayList<>(fiveFactors);
      Iterable<Long> result = () -> new StreamIntersectMerger(List.of(
          stream1.iterator(),
          stream2.iterator(),
          stream3.iterator()));
      MatcherAssert.assertThat(result, Matchers.contains(30L, 60L, 90L));
    }
  }

  @Nested
  public class WhenSomeStreamsAreEmpty {
    private final List<Long> oneToOneHundred = LongStream.rangeClosed(1, 100)
        .boxed()
        .collect(Collectors.toList());
    private final List<Long> emptyStream = Collections.emptyList();

    @Test
    public void testOnlyEmptyStream() {
      Iterable<Long> result = () -> new StreamIntersectMerger(List.of(emptyStream.iterator()));
      MatcherAssert.assertThat(result, Matchers.emptyIterable());
    }

    @Test
    public void testOneEmptyStream() {
      Iterable<Long> result = () -> new StreamIntersectMerger(List.of(
          oneToOneHundred.iterator(),
          emptyStream.iterator()));
      MatcherAssert.assertThat(result, Matchers.emptyIterable());
    }
  }

  // TODO: This should probably throw an exception
  @Nested
  public class WhenInputIsNotSorted {

  }
}
