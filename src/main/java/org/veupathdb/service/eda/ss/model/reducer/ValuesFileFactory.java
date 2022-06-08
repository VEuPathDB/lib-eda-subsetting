package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.filter.SingleValueFilter;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueConverter;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueWithIdSerializer;

import java.io.IOException;
import java.nio.file.Path;

public class ValuesFileFactory {

  private final Path entityRepositoryDir;

  public ValuesFileFactory(Path entityRepositoryDir) {
    this.entityRepositoryDir = entityRepositoryDir;
  }

  /**
   * Open a binary file based on the filter passed in. Passes the predicate associated with the filter to the
   * {@link FilteredValueFile} which produces a stream of IDs based on the filter.
   * @param filter Filter for which to create a {@link FilteredValueFile}.
   * @param <V> Type of variable {@link VariableWithValues}
   * @param <T> Type of value associated with {@link V}
   * @throws IOException
   */
  public <V, T extends VariableWithValues<V>> FilteredValueFile<V> createFromFilter(
      SingleValueFilter<V, T> filter) throws IOException {
    /**
     * TODO Read metadata from files here for Long vs. Integer or String bytes length?
     */
    ValueConverter<V> serializer = filter.getVariable().getValueConverter();
    return new FilteredValueFile<>(
        constructPath(filter),
        filter.getPredicate(),
        new ValueWithIdSerializer<>(serializer));
  }

  private Path constructPath(SingleValueFilter<?,?> filter) {
    // TODO: Study ID instead of StudyAbbrev?
    return Path.of(entityRepositoryDir.toString(),
        filter.getEntity().getStudyAbbrev(),
        filter.getEntity().getId(),
        filter.getVariable().getId());
  }
}