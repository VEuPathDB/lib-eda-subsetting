package org.veupathdb.service.eda.ss.model.reducer;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.List;

public class StreamUnionMergerTest {

  @Test
  public void testTwoStreams() {
    List<Long> stream1 = List.of(1L, 3L, 4L, 6L, 12L);
    List<Long> stream2 = List.of(1L, 2L, 4L, 4L, 8L);
    Iterable<Long> merger = () -> new StreamUnionMerger(List.of(stream1.iterator(), stream2.iterator()));
    MatcherAssert.assertThat(merger, Matchers.contains(1L, 2L, 3L, 4L, 6L, 8L, 12L));
  }

  @Test
  public void testThreeStreams() {
    List<Long> stream1 = List.of(1L, 3L, 4L, 6L, 12L);
    List<Long> stream2 = List.of(1L, 2L, 4L, 4L, 8L);
    List<Long> stream3 = List.of(1L, 2L, 7L, 8L);

    Iterable<Long> merger = () -> new StreamUnionMerger(List.of(stream1.iterator(),
        stream2.iterator(), stream3.iterator()));
    MatcherAssert.assertThat(merger, Matchers.contains(1L, 2L, 3L, 4L, 6L, 7L, 8L, 12L));
  }
}