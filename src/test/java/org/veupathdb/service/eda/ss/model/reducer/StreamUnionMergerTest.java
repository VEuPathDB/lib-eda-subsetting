package org.veupathdb.service.eda.ss.model.reducer;

import org.junit.jupiter.api.Test;

import java.util.List;

public class StreamUnionMergerTest {

  @Test
  public void testTwoStreams() {
    List<Long> stream1 = List.of(1L, 3L, 4L, 6L, 12L);
    List<Long> stream2 = List.of(1L, 2L, 4L, 4L, 8L);
    Iterable<Long> merger = () -> new StreamUnionMerger(List.of(stream1.iterator(), stream2.iterator()));
    merger.iterator().forEachRemaining(System.out::println);
  }

  @Test
  public void testThreeStreams() {
    List<Long> stream1 = List.of(1L, 3L, 4L, 6L, 12L);
    List<Long> stream2 = List.of(1L, 2L, 4L, 4L, 8L);
    List<Long> stream3 = List.of(1L, 2L, 7L, 8L);

    Iterable<Long> merger = () -> new StreamUnionMerger(List.of(stream1.iterator(),
        stream2.iterator(), stream3.iterator()));
    merger.iterator().forEachRemaining(System.out::println);
  }
}