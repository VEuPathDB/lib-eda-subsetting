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

  public <U, V extends VariableWithValues> FilteredValueStream<U> createFromFilter(
      SingleValueFilter<V, U> filter) throws IOException {
    /**
     * TODO Read metadata from files here for Long vs. Integer or String bytes length?
     */
    ValueConverter<U> serializer = filter.getVariable().getType().getValueConverter();
    return new FilteredValueStream<>(
        constructPath(filter),
        filter.getPredicate(),
        new ValueWithIdSerializer<>(serializer));
  }

  private Path constructPath(SingleValueFilter filter) {
    // TODO: Study ID instead of StudyAbbrev?
    return Path.of(entityRepositoryDir.toString(),
        filter.getEntity().getStudyAbbrev(),
        filter.getEntity().getId(),
        filter.getVariable().getId());
  }
}
