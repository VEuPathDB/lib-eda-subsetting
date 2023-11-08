package org.veupathdb.service.eda.subset.model.reducer.ancestor;

import org.gusdb.fgputil.iterator.CloseableIterator;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;

import java.util.Collections;
import java.util.List;

public class AncestorExpanderTest {

  @Test
  public void testFirstAndLastAncestorIncluded() {
    List<VariableValueIdPair<Long>> descendantStream = List.of(
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
    MatcherAssert.assertThat(() -> constructExpander(descendantStream, entityStream),
        Matchers.contains(1L, 2L, 3L, 8L, 9L, 10L));
  }

  @Test
  public void testFirstIdMissingFromDescendantStream() {
    List<VariableValueIdPair<Long>> descendantStream = List.of(
        new VariableValueIdPair<>(1L, 2L),
        new VariableValueIdPair<>(2L, 2L),
        new VariableValueIdPair<>(3L, 2L),
        new VariableValueIdPair<>(4L, 2L),
        new VariableValueIdPair<>(5L, 2L),
        new VariableValueIdPair<>(6L, 2L),
        new VariableValueIdPair<>(7L, 2L),
        new VariableValueIdPair<>(8L, 4L),
        new VariableValueIdPair<>(9L, 4L),
        new VariableValueIdPair<>(10L, 4L)
    );
    List<Long> entityStream = List.of(
        1L,
        4L
    );
    MatcherAssert.assertThat(() -> constructExpander(descendantStream, entityStream),
        Matchers.contains(8L, 9L, 10L));
  }

  @Test
  public void testFirstIdMissingFromEntityStream() {
    List<VariableValueIdPair<Long>> descendantStream = List.of(
        new VariableValueIdPair<>(1L, 1L),
        new VariableValueIdPair<>(2L, 1L),
        new VariableValueIdPair<>(3L, 2L),
        new VariableValueIdPair<>(4L, 2L),
        new VariableValueIdPair<>(5L, 2L),
        new VariableValueIdPair<>(6L, 2L),
        new VariableValueIdPair<>(7L, 2L),
        new VariableValueIdPair<>(8L, 4L),
        new VariableValueIdPair<>(9L, 4L),
        new VariableValueIdPair<>(10L, 4L)
    );
    List<Long> entityStream = List.of(
        2L,
        4L
    );
    MatcherAssert.assertThat(() -> constructExpander(descendantStream, entityStream),
        Matchers.contains(3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));
  }


  @Test
  public void testEmptyList() {
    Iterable<Long> expander = () -> constructExpander(Collections.emptyList(), Collections.emptyList());
    MatcherAssert.assertThat(expander, Matchers.emptyIterable());
  }

  @Test
  public void testSecondToLastIncluded() {
    List<VariableValueIdPair<Long>> descendantStream = List.of(
        new VariableValueIdPair<>(1L, 1L),
        new VariableValueIdPair<>(2L, 1L),
        new VariableValueIdPair<>(3L, 2L),
        new VariableValueIdPair<>(4L, 2L),
        new VariableValueIdPair<>(5L, 3L),
        new VariableValueIdPair<>(6L, 3L),
        new VariableValueIdPair<>(7L, 3L),
        new VariableValueIdPair<>(8L, 4L),
        new VariableValueIdPair<>(9L, 4L),
        new VariableValueIdPair<>(10L, 4L)
    );
    List<Long> entityStream = List.of(
        2L, 3L
    );
    MatcherAssert.assertThat(() -> constructExpander(descendantStream, entityStream),
        Matchers.contains(3L, 4L, 5L, 6L, 7L));

  }

  @Test
  public void testSecondToLastEntityIncluded() {
    List<VariableValueIdPair<Long>> descendantStream = List.of(
        new VariableValueIdPair<>(1L, 1L),
        new VariableValueIdPair<>(2L, 1L),
        new VariableValueIdPair<>(3L, 2L),
        new VariableValueIdPair<>(4L, 2L),
        new VariableValueIdPair<>(5L, 3L),
        new VariableValueIdPair<>(6L, 3L),
        new VariableValueIdPair<>(7L, 3L),
        new VariableValueIdPair<>(8L, 4L),
        new VariableValueIdPair<>(9L, 4L),
        new VariableValueIdPair<>(10L, 4L),
        new VariableValueIdPair<>(11L, 5L),
        new VariableValueIdPair<>(12L, 5L)
    );
    List<Long> entityStream = List.of(
        2L, 3L, 4L
    );
    constructExpander(descendantStream, entityStream).forEachRemaining(System.out::println);
    MatcherAssert.assertThat(() -> constructExpander(descendantStream, entityStream),
        Matchers.contains(3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));

  }

  @Test
  public void testEmptyEntityStream() {
    List<VariableValueIdPair<Long>> descendantStream = List.of(
        new VariableValueIdPair<>(1L, 1L),
        new VariableValueIdPair<>(2L, 1L),
        new VariableValueIdPair<>(3L, 2L),
        new VariableValueIdPair<>(4L, 2L),
        new VariableValueIdPair<>(5L, 3L),
        new VariableValueIdPair<>(6L, 3L),
        new VariableValueIdPair<>(7L, 3L),
        new VariableValueIdPair<>(8L, 4L),
        new VariableValueIdPair<>(9L, 4L),
        new VariableValueIdPair<>(10L, 4L),
        new VariableValueIdPair<>(11L, 5L),
        new VariableValueIdPair<>(12L, 5L)
    );
    List<Long> entityStream = List.of();
    Iterable<Long> outputStream = () -> constructExpander(descendantStream, entityStream);

    MatcherAssert.assertThat(outputStream, Matchers.emptyIterable());
  }


  private CloseableIterator<Long> constructExpander(List<VariableValueIdPair<Long>> ancestorStream,
                                                    List<Long> entityStream) {
    return new AncestorExpander(CloseableIterator.of(ancestorStream.iterator()), CloseableIterator.of(entityStream.iterator()));
  }

}