package org.veupathdb.service.eda.ss.model.reducer;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.List;

public class EntityJoinerRootTest {

  @Test
  public void testSingleFilteredStreamOutputAllValues() {
    final List<Long> idIndexes = List.of(1L, 2L, 3L, 4L, 5L);
    final List<VariableValueIdPair<String>> varValues = List.of(
        constructPair(1L, "1"),
        constructPair(2L, "2"),
        constructPair(3L, "3"),
        constructPair(4L, "4"),
        constructPair(5L, "5")
    );
    Iterable<String> joinerRoot = () -> new EntityJoinerRoot<>(List.of(idIndexes.iterator()), varValues.iterator()).reduce();
    MatcherAssert.assertThat(joinerRoot, Matchers.contains("1", "2", "3", "4", "5"));
  }

  @Test
  public void testSingleFilteredStreamOutputSubset() {
    final List<Long> idIndexes = List.of(2L, 4L);
    final List<VariableValueIdPair<String>> varValues = List.of(
        constructPair(1L, "1"),
        constructPair(2L, "2"),
        constructPair(3L, "3"),
        constructPair(4L, "4"),
        constructPair(5L, "5")
    );
    Iterable<String> joinerRoot = () -> new EntityJoinerRoot<>(List.of(idIndexes.iterator()), varValues.iterator()).reduce();
    MatcherAssert.assertThat(joinerRoot, Matchers.contains("2", "4"));
  }

  @Test
  public void testMultipleFilteredStreamOutputSubset() {
    final List<Long> stream1 = List.of(1L, 2L, 3L, 4L);
    final List<Long> stream2 = List.of(3L, 4L, 5L);
    final List<VariableValueIdPair<String>> varValues = List.of(
        constructPair(1L, "1"),
        constructPair(2L, "2"),
        constructPair(3L, "3"),
        constructPair(4L, "4"),
        constructPair(5L, "5")
    );
    Iterable<String> joinerRoot = () -> new EntityJoinerRoot<>(List.of(stream1.iterator(), stream2.iterator()),
        varValues.iterator()).reduce();
    MatcherAssert.assertThat(joinerRoot, Matchers.contains("3", "4"));
  }


  private VariableValueIdPair<String> constructPair(long idIndex, String value) {
    return new VariableValueIdPair<>(idIndex, value);
  }
}
