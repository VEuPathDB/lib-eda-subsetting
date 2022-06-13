package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.Iterator;
import java.util.List;

public class AncestorExpanderTest {

  @Test
  public void test() {
    List<VariableValueIdPair<Long>> ancestorStream = List.of(
        new VariableValueIdPair<>(1L, 1L),
        new VariableValueIdPair<>(2L, 1L),
        new VariableValueIdPair<>(3L, 1L),
        new VariableValueIdPair<>(4L, 2L),
        new VariableValueIdPair<>(5L, 2L),
        new VariableValueIdPair<>(6L, 2L),
        new VariableValueIdPair<>(7L, 2L),
        new VariableValueIdPair<>(8L, 4L),
        new VariableValueIdPair<>(9L, 4L),
        new VariableValueIdPair<>(10L, 4L)
    );
    List<Long> entityStream = List.of(1L, 4L);
    MatcherAssert.assertThat(() -> constructExpander(ancestorStream, entityStream),
        Matchers.contains(1L, 2L, 3L, 8L, 9L, 10L));
  }

  private Iterator<Long> constructExpander(List<VariableValueIdPair<Long>> ancestorStream,
                                           List<Long> entityStream) {
    return new AncestorExpander(ancestorStream.iterator(), entityStream.iterator());
  }

}