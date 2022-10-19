package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class DescendantCollapserTest {

  @Test
  public void testOneChildForEachParentIncluded() {
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
    List<Long> entityStream = List.of(1L, 3L, 5L);
    MatcherAssert.assertThat(() -> constructCollapser(ancestorStream, entityStream),
        Matchers.contains(1L, 2L));
  }

  @Test
  public void testMultipleAncestorsExcluded() {
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
        new VariableValueIdPair<>(10L, 4L),
        new VariableValueIdPair<>(12L, 5L),
        new VariableValueIdPair<>(15L, 6L)

    );
    List<Long> entityStream = List.of(1L, 2L, 3L, 5L, 10L, 15L);
    MatcherAssert.assertThat(() -> constructCollapser(ancestorStream, entityStream),
        Matchers.contains(1L, 2L, 4L, 6L));
  }

  @Test
  public void testExcludeFirstAncestor() {
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
        new VariableValueIdPair<>(10L, 4L),
        new VariableValueIdPair<>(12L, 5L),
        new VariableValueIdPair<>(15L, 6L)
    );
    List<Long> entityStream = List.of(5L, 10L, 15L);
    MatcherAssert.assertThat(() -> constructCollapser(ancestorStream, entityStream),
        Matchers.contains(2L, 4L, 6L));
  }

  @Test
  public void testExcludeFirstEntity() {
    List<VariableValueIdPair<Long>> ancestorStream = List.of(
        new VariableValueIdPair<>(4L, 2L),
        new VariableValueIdPair<>(5L, 2L),
        new VariableValueIdPair<>(6L, 2L),
        new VariableValueIdPair<>(7L, 2L),
        new VariableValueIdPair<>(8L, 4L),
        new VariableValueIdPair<>(9L, 4L),
        new VariableValueIdPair<>(10L, 4L),
        new VariableValueIdPair<>(12L, 5L),
        new VariableValueIdPair<>(15L, 6L)
    );
    List<Long> entityStream = List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 15L);
    MatcherAssert.assertThat(() -> constructCollapser(ancestorStream, entityStream),
        Matchers.contains(2L, 4L, 6L));
  }

  @Test
  public void testSecondToLastIncluded() {
    List<VariableValueIdPair<Long>> participantDescendants = List.of(
        new VariableValueIdPair<>(4L, 2L),
        new VariableValueIdPair<>(5L, 2L),
        new VariableValueIdPair<>(6L, 2L),
        new VariableValueIdPair<>(7L, 2L),
        new VariableValueIdPair<>(8L, 4L),
        new VariableValueIdPair<>(9L, 4L),
        new VariableValueIdPair<>(10L, 4L),
        new VariableValueIdPair<>(12L, 5L),
        new VariableValueIdPair<>(15L, 6L),
        new VariableValueIdPair<>(16L, 7L),
        new VariableValueIdPair<>(19L, 19L),
        new VariableValueIdPair<>(20L, 20L)
    );
    List<Long> entityStream = List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 15L, 20L);
    MatcherAssert.assertThat(() -> constructCollapser(participantDescendants, entityStream),
        Matchers.contains(2L, 4L, 6L, 20L));
  }

  @Test
  public void testEmptyList() {
    Iterable<Long> expander = () -> constructCollapser(Collections.emptyList(), Collections.emptyList());
    MatcherAssert.assertThat(expander, Matchers.emptyIterable());
  }

  private Iterator<Long> constructCollapser(List<VariableValueIdPair<Long>> ancestorStream,
                                            List<Long> entityStream) {
    return new DescendantCollapser(ancestorStream.iterator(), entityStream.iterator());
  }
}