package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.SingleValueFilter;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;
import org.veupathdb.service.eda.ss.model.variable.binary.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;

public class ValuesFileFactory {
  private final Path entityRepositoryDir;
  private final BinaryFilesManager binaryFilesManager;

  public ValuesFileFactory(Path entityRepositoryDir) {
    this.entityRepositoryDir = entityRepositoryDir;
    this.binaryFilesManager = new BinaryFilesManager(entityRepositoryDir);
  }

  /**
   * Open a binary file based on the filter passed in. Passes the predicate associated with the filter to the
   * {@link FilteredValueFile} which produces a stream of IDs based on the filter.
   * @param filter Filter for which to create a {@link FilteredValueFile}.
   * @param <V> Type of variable {@link VariableWithValues}
   * @param <T> Type of value associated with {@link V}
   * @throws IOException
   */
  public <V, T extends VariableWithValues<V>> FilteredValueFile<V, Long> createFromFilter(
      SingleValueFilter<V, T> filter) throws IOException {
    /**
     * TODO Read metadata from files here for Long vs. Integer or String bytes length?
     */
    BinaryConverter<V> serializer = filter.getVariable().getBinaryConverter();
    return new FilteredValueFile<>(
        constructPath(filter),
        filter.getPredicate(),
        new ValueWithIdDeserializer<>(serializer),
        VariableValueIdPair::getIdIndex);
  }

  public <V, T extends VariableWithValues<V>> FilteredValueFile<V, Long> createFromFilter(
      SingleValueFilter<V, T> filter, Study study) throws IOException {
    /**
     * TODO Read metadata from files here for Long vs. Integer or String bytes length?
     */
    BinaryConverter<V> serializer = filter.getVariable().getBinaryConverter();
    return new FilteredValueFile<>(
        binaryFilesManager.getVariableFile(study,
            filter.getEntity(),
            filter.getVariable(),
            BinaryFilesManager.Operation.READ),
        filter.getPredicate(),
        new ValueWithIdDeserializer<>(serializer),
        VariableValueIdPair::getIdIndex);
  }


  public FilteredValueFile<Long, Long> createAncestorsDataStream(
      Study study,
      Entity ancestor,
      Entity descendant
  ) throws IOException {
    int index = descendant.getAncestorEntities().indexOf(ancestor);
    final LongValueConverter longValueConverter = new LongValueConverter();
    final ListConverter<Long> ancestorTupleConverter = new ListConverter<>(longValueConverter, descendant.getAncestorEntities().size());
    final AncestorDeserializer deserializer = new AncestorDeserializer(ancestorTupleConverter, index);
    return new FilteredValueFile<>(
        binaryFilesManager.getAncestorFile(study, descendant, BinaryFilesManager.Operation.READ),
        x -> true,
        deserializer,
        VariableValueIdPair::getIdIndex
    );
  }

  public <V> FilteredValueFile<V, VariableValueIdPair<?>> createValuesFile(
      Study study,
      VariableWithValues<V> variable) throws IOException {
    /**
     * TODO Read metadata from files here for Long vs. Integer or String bytes length?
     */
    BinaryConverter<V> serializer = variable.getBinaryConverter();
    return new FilteredValueFile(
        binaryFilesManager.getVariableFile(study,
            variable.getEntity(),
            variable,
            BinaryFilesManager.Operation.READ),
        x -> true,
        new ValueWithIdDeserializer<>(serializer),
        Function.identity());
  }

  private Path constructPath(SingleValueFilter<?,?> filter) {
    // TODO: Study ID instead of StudyAbbrev?
    return Path.of(entityRepositoryDir.toString(),
        filter.getEntity().getStudyAbbrev(),
        filter.getEntity().getId(),
        filter.getVariable().getId());
  }
}
