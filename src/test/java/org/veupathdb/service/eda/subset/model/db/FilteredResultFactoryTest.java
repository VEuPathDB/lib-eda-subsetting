package org.veupathdb.service.eda.subset.model.db;

import org.gusdb.fgputil.iterator.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.filter.NumberRangeFilter;
import org.veupathdb.service.eda.subset.model.filter.SingleValueFilter;
import org.veupathdb.service.eda.subset.model.reducer.BinaryValuesStreamer;
import org.veupathdb.service.eda.subset.model.variable.NumberVariable;
import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;
import org.veupathdb.service.eda.subset.testutil.TestDataProvider;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

public class FilteredResultFactoryTest {

  private BinaryValuesStreamer valuesStreamer;
  private DataSource ds;

  @BeforeEach
  public void before() {
    this.valuesStreamer = Mockito.mock(BinaryValuesStreamer.class);
    this.ds = Mockito.mock(DataSource.class);
  }

  @Test
  public void singleEntityTest() throws Exception {
    final String studyId = "StudyID";
    final String studyAbbrev = "Abbrev";

    final Entity root = TestDataProvider.constructEntity();
    final VariableWithValues<Long> var = TestDataProvider.constructIntVariable(root);
    root.addVariable(var);

    final Study study = new TestDataProvider.StudyBuilder(studyId, studyAbbrev)
      .withRoot(root)
      .build();

    // Mock a stream of 5 entities with a single variable
    Mockito.when(valuesStreamer.streamUnfilteredEntityIdIndexes(study, root))
      .thenReturn(CloseableIterator.of(List.of(1L, 2L, 3L, 4L, 5L).iterator()));

    // Mock the stream's variable values corresponding to entities with ID indexes 1-5
    Mockito.when(valuesStreamer.streamUnformattedIdValueBinaryPairs(Mockito.eq(study), Mockito.eq(var)))
      .thenReturn(CloseableIterator.of(List.of(
        new VariableValueIdPair<>(1L, "100"),
        new VariableValueIdPair<>(2L, "101"),
        new VariableValueIdPair<>(3L, "102"),
        new VariableValueIdPair<>(4L, "103"),
        new VariableValueIdPair<>(5L, "104")
      ).iterator()));

    // Mock the stream's human-readable entity IDs' mapping to ID index.
    Mockito.when(valuesStreamer.streamIdMapAsStrings(root, study)).thenReturn(CloseableIterator.of(List.of(
      new VariableValueIdPair<>(1L, List.of("entity-1")),
      new VariableValueIdPair<>(2L, List.of("entity-2")),
      new VariableValueIdPair<>(3L, List.of("entity-3")),
      new VariableValueIdPair<>(4L, List.of("entity-4")),
      new VariableValueIdPair<>(5L, List.of("entity-5"))
    ).iterator()));

    try (CloseableIterator<Map<String, String>> out = FilteredResultFactory.tabularSubsetIterator(
      study,
      root,
      List.of(var),
      List.of(),
      valuesStreamer,
      true,
      ds,
      "fake-schema"
    )) {
      Assertions.assertEquals("100", out.next().get(var.getDotNotation()));
      Assertions.assertEquals("101", out.next().get(var.getDotNotation()));
      Assertions.assertEquals("102", out.next().get(var.getDotNotation()));
      Assertions.assertEquals("103", out.next().get(var.getDotNotation()));
      Assertions.assertEquals("104", out.next().get(var.getDotNotation()));
    }
  }

  /**
   * Test producing a tabular subset with a simple two-entity tree.
   *
   * Filter based on a numeric variable on the child-entity and output variable values on the parent-entity.
   */
  @Test
  public void multipleEntityTestFiltered() throws Exception {
    final String studyId = "StudyID";
    final String studyAbbrev = "Abbrev";

    final Entity root = TestDataProvider.constructEntity();
    final Entity child = TestDataProvider.constructEntity();

    final NumberVariable<Long> var1 = TestDataProvider.constructIntVariable(root);
    final NumberVariable<Long> var2 = TestDataProvider.constructIntVariable(root);

    root.addVariable(var1);
    child.addVariable(var2);

    final SingleValueFilter<Long, NumberVariable<Long>> wayTooBigFilter =
      new NumberRangeFilter<>("stub", child, var2, 1000L, 1000000L);

    final Study study = new TestDataProvider.StudyBuilder(studyId, studyAbbrev)
      .withRoot(root)
      .addEntity(child, root.getId())
      .build();

    // Mock a stream of 5 entities with a single variable
    Mockito.when(valuesStreamer.streamFilteredEntityIdIndexes(wayTooBigFilter, study))
      .thenReturn(CloseableIterator.of(List.of(11L, 31L, 32L, 41L).iterator()));

    Mockito.when(valuesStreamer.streamUnfilteredEntityIdIndexes(study, root))
      .thenReturn(CloseableIterator.of(List.of(1L, 2L, 3L, 4L, 5L).iterator()));

    Mockito.when(valuesStreamer.streamAncestorIds(child, study, 1)).thenReturn(CloseableIterator.of(List.of(
      new VariableValueIdPair<>(11L, 1L),
      new VariableValueIdPair<>(12L, 1L),
      new VariableValueIdPair<>(13L, 1L),
      new VariableValueIdPair<>(21L, 2L),
      new VariableValueIdPair<>(22L, 2L),
      new VariableValueIdPair<>(23L, 2L),
      new VariableValueIdPair<>(31L, 3L),
      new VariableValueIdPair<>(32L, 3L),
      new VariableValueIdPair<>(41L, 4L),
      new VariableValueIdPair<>(51L, 5L),
      new VariableValueIdPair<>(52L, 5L),
      new VariableValueIdPair<>(53L, 5L)
    ).iterator()));

    // Mock the parent stream's variable values corresponding to entities with ID indexes 1-5
    Mockito.when(valuesStreamer.streamUnformattedIdValueBinaryPairs(Mockito.eq(study), Mockito.eq(var1)))
      .thenReturn(CloseableIterator.of(List.of(
        new VariableValueIdPair<>(1L, "100"),
        new VariableValueIdPair<>(2L, "101"),
        new VariableValueIdPair<>(3L, "102"),
        new VariableValueIdPair<>(4L, "103"),
        new VariableValueIdPair<>(5L, "104")
      ).iterator()));

    // Mock the parent stream's human-readable entity IDs' mapping to ID index.
    Mockito.when(valuesStreamer.streamIdMapAsStrings(root, study)).thenReturn(CloseableIterator.of(List.of(
      new VariableValueIdPair<>(1L, List.of("parent-1")),
      new VariableValueIdPair<>(2L, List.of("parent-2")),
      new VariableValueIdPair<>(3L, List.of("parent-3")),
      new VariableValueIdPair<>(4L, List.of("parent-4")),
      new VariableValueIdPair<>(5L, List.of("parent-5"))
    ).iterator()));

    // Mock the child stream's human-readable IDs
    Mockito.when(valuesStreamer.streamIdMapAsStrings(child, study)).thenReturn(CloseableIterator.of(List.of(
      new VariableValueIdPair<>(11L, List.of("child-11", "parent-1")),
      new VariableValueIdPair<>(12L, List.of("child-12", "parent-1")),
      new VariableValueIdPair<>(13L, List.of("child-13", "parent-1")),
      new VariableValueIdPair<>(21L, List.of("child-21", "parent-2")),
      new VariableValueIdPair<>(22L, List.of("child-22", "parent-2")),
      new VariableValueIdPair<>(23L, List.of("child-23", "parent-2")),
      new VariableValueIdPair<>(31L, List.of("child-31", "parent-3")),
      new VariableValueIdPair<>(32L, List.of("child-32", "parent-3")),
      new VariableValueIdPair<>(33L, List.of("child-33", "parent-3")),
      new VariableValueIdPair<>(41L, List.of("child-41", "parent-4")),
      new VariableValueIdPair<>(42L, List.of("child-42", "parent-4")),
      new VariableValueIdPair<>(43L, List.of("child-43", "parent-4")),
      new VariableValueIdPair<>(51L, List.of("child-51", "parent-5")),
      new VariableValueIdPair<>(52L, List.of("child-52", "parent-5")),
      new VariableValueIdPair<>(53L, List.of("child-53", "parent-5"))
    ).iterator()));

    try (CloseableIterator<Map<String, String>> out = FilteredResultFactory.tabularSubsetIterator(
      study,
      root,
      List.of(var1),
      List.of(wayTooBigFilter),
      valuesStreamer,
      true,
      ds,
      "fake-schema"
    )) {

      // We filtered away all children of entity "101" and "104", hence their absence from expectation.
      Assertions.assertEquals("100", out.next().get(var1.getDotNotation()));
      Assertions.assertEquals("102", out.next().get(var1.getDotNotation()));
      Assertions.assertEquals("103", out.next().get(var1.getDotNotation()));
    }
  }
}
