package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.SingleValueFilter;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;
import org.veupathdb.service.eda.ss.model.variable.binary.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;

public class ValuesFileFactory {
  private final BinaryFilesManager binaryFilesManager;

  public ValuesFileFactory(Path entityRepositoryDir) {
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
      SingleValueFilter<V, T> filter, Study study) throws IOException {
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

  public <V> FilteredValueFile<V, VariableValueIdPair<?>> createValuesFile(
      Study study,
      VariableWithValues<V> variable) throws IOException {
    BinaryConverter<V> serializer = variable.getBinaryConverter();
    return new FilteredValueFile(
        binaryFilesManager.getVariableFile(study,
            variable.getEntity(),
            variable,
            BinaryFilesManager.Operation.READ),
        x -> true, // Always return true, extract all ID index pairs and variable values.
        new ValueWithIdDeserializer<>(serializer),
        Function.identity()); // Provide a stream of entire VariableValueIdPair objects.
  }
}
