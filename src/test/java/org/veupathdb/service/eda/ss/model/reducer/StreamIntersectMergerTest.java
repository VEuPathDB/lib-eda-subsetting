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
  public class WhenThereAreNoDuplicates {
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
      final List<Long> stream1 = new ArrayList<>(oneToOneHundred);
      final List<Long> stream2 = new ArrayList<>(oneToOneHundred);
      Iterable<Long> iterable = () -> new StreamIntersectMerger(List.of(stream1.iterator(), stream2.iterator()));
      MatcherAssert.assertThat(iterable, Matchers.iterableWithSize(100));
    }

    @Test
    public void testTwoDistinctStreams() {
      final List<Long> stream1 = new ArrayList<>(evenNumbers);
      final List<Long> stream2 = new ArrayList<>(oddNumbers);
      Iterable<Long> iterable = () -> new StreamIntersectMerger(List.of(stream1.iterator(), stream2.iterator()));
      MatcherAssert.assertThat(iterable, Matchers.emptyIterable());
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
      // Common multiples of 2, 3, and 5 between 1 and 100 are 30, 60 and 90.
      MatcherAssert.assertThat(result, Matchers.contains(30L, 60L, 90L));
    }

    @Test
    public void testInputStreamsNotSortedByStartingElement() {
      final List<Long> stream1 = new ArrayList<>(fiveFactors);
      final List<Long> stream2 = new ArrayList<>(fiveFactors);
      final List<Long> stream3 = new ArrayList<>(threeFactors);
      Iterable<Long> result = () -> new StreamIntersectMerger(List.of(
          stream1.iterator(),
          stream2.iterator(),
          stream3.iterator()));
      // Common multiples of 2, 3, and 5 between 1 and 100 are 30, 60 and 90.
      MatcherAssert.assertThat(result, Matchers.contains(15L, 30L, 45L, 60L, 75L, 90L));
    }
  }

  @Nested
  public class WhenThereAreDuplicates {
    private final List<Long> stream1 = List.of(1L, 1L, 1L, 2L, 2L, 4L, 4L, 8L);
    private final List<Long> stream2 = List.of(1L, 1L, 2L, 4L, 5L, 5L, 8L);

    @Test
    public void testDupes1() {
      Iterable<Long> result = () -> new StreamIntersectMerger(List.of(
          stream1.iterator(),
          stream2.iterator()));
      MatcherAssert.assertThat(result, Matchers.contains(1L, 2L, 4L, 8L));
    }

    @Test
    public void testDupes2() {
      final List<Long> s1 = List.of(1L, 2L, 3L, 4L, 5L, 10L);
      final List<Long> s2 = List.of(2L, 5L, 6L, 10L);
      Iterable<Long> result = () -> new StreamIntersectMerger(List.of(
          s1.iterator(),
          s2.iterator()));
      MatcherAssert.assertThat(result, Matchers.contains(2L, 5L, 10L));
    }

    @Test
    public void testDupes3() {
      final List<Long> s1 = List.of(1L, 2L, 3L, 4L, 5L, 9L, 10L);
      final List<Long> s2 = List.of(2L, 5L, 6L, 9L, 10L);
      Iterable<Long> result = () -> new StreamIntersectMerger(List.of(
          s1.iterator(),
          s2.iterator()));
      MatcherAssert.assertThat(result, Matchers.contains(2L, 5L, 9L, 10L));
    }

    @Test
    public void testDuplicatesSingleStream() {
      final List<Long> s1 = List.of(1L, 1L, 2L, 3L, 3L, 9L, 10L);
      Iterable<Long> result = () -> new StreamIntersectMerger(List.of(s1.iterator()));
      MatcherAssert.assertThat(result, Matchers.contains(1L, 2L, 3L, 9L, 10L));
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
}
