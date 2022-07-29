package org.veupathdb.service.eda.ss.model.reducer;

import org.gusdb.fgputil.functional.Functions;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.MultiFilter;
import org.veupathdb.service.eda.ss.model.filter.MultiFilterSubFilter;
import org.veupathdb.service.eda.ss.model.filter.SingleValueFilter;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;
import org.veupathdb.service.eda.ss.model.variable.binary.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BinaryValuesStreamer {
  private static final LongValueConverter LONG_VALUE_CONVERTER = new LongValueConverter();

  private final BinaryFilesManager binaryFilesManager;

  public BinaryValuesStreamer(Path entityRepositoryDir) {
    this.binaryFilesManager = new BinaryFilesManager(entityRepositoryDir);
  }

  /**
   * Stream entity IDs from a binary file based on the filter passed in. Passes the predicate associated with the filter
   * to the {@link FilteredValueIterator} which applies the predicate to each variable value and streams entity IDs
   * whose values match the predicate.
   * @param filter Filter for which to create a {@link FilteredValueIterator}.
   * @param <V> Type of variable {@link VariableWithValues}
   * @param <T> Type of value associated with {@link V}
   * @throws IOException if there is a failure to open the binary file.
   */
  public <V, T extends VariableWithValues<V>> FilteredValueIterator<V, Long> streamFilteredValues(
      SingleValueFilter<V, T> filter, Study study) throws IOException {
    BinaryConverter<V> serializer = filter.getVariable().getBinaryConverter();
    return new FilteredValueIterator<>(
        binaryFilesManager.getVariableFile(study,
            filter.getEntity(),
            filter.getVariable(),
            BinaryFilesManager.Operation.READ),
        filter.getPredicate(),
        new ValueWithIdDeserializer<>(serializer),
        VariableValueIdPair::getIdIndex);
  }

  public FilteredValueIterator<String, Long> streamMultiSubFilterValues(MultiFilterSubFilter subFilter,
                                                                       Study study,
                                                                       MultiFilter filter) throws IOException {
    return streamFilteredValues(filter.getFilter(subFilter), study);
  }

  public Iterator<Long> streamMultiFilteredValues(
      MultiFilter filter, Study study) throws IOException {
    List<Iterator<Long>> idStreams = filter.getSubFilters().stream()
        .map(Functions.fSwallow(subFilter -> streamMultiSubFilterValues(subFilter, study, filter)))
        .collect(Collectors.toList());
    if (filter.getOperation() == MultiFilter.MultiFilterOperation.UNION) {
      return new StreamUnionMerger(idStreams); // Intersect depending on operation.
    } else { // operation == MultiFilter.MultiFilterOperation.UNION.INTERSECT
      return new StreamIntersectMerger(idStreams);
    }
  }

  /**
   * Streams tuples of all entity ID indexes and the variable values associate with the variable passed in.
   * @param study The study that the variable belongs to. Used to locate the binary file.
   * @param variable The variable whose values are requested.
   * @param <V> The type of the variable values.
   * @return An iterator  all {@link VariableValueIdPair}s containing all ID indexes and associated variable values.
   * @throws IOException if there is a failure to open the binary file.
   */
  public <V> FilteredValueIterator<V, VariableValueIdPair<String>> streamIdValuePairs(
      Study study,
      VariableWithValues<V> variable) throws IOException {
    Function<VariableValueIdPair<V>, VariableValueIdPair<String>> extractor = pair -> new VariableValueIdPair<>(
        pair.getIdIndex(), variable.valueToString(pair.getValue()));
    BinaryConverter<V> serializer = variable.getBinaryConverter();
    return new FilteredValueIterator(
        binaryFilesManager.getVariableFile(study,
            variable.getEntity(),
            variable,
            BinaryFilesManager.Operation.READ),
        x -> true, // Always return true, extract all ID index pairs and variable values.
        new ValueWithIdDeserializer<>(serializer),
        extractor); // Provide a stream of entire VariableValueIdPair objects.
  }

  /**
   *
   * @param descendant Entity for which to retrieve ancestors stream.
   * @param study Study the entity belongs to.
   * @return Stream of ancestor IDs.
   * @throws IOException
   */
  public Iterator<VariableValueIdPair<List<Long>>> streamAncestorIds(Entity descendant,
                                                                     Study study) throws IOException {
    Path path = binaryFilesManager.getAncestorFile(study, descendant, BinaryFilesManager.Operation.READ);
    final ListConverter<Long> listConverter = new ListConverter<>(LONG_VALUE_CONVERTER, descendant.getAncestorEntities().size());
    final ValueWithIdDeserializer<List<Long>> ancestorsWithId = new ValueWithIdDeserializer<>(listConverter);
    return new FilteredValueIterator<>(path,
        x -> true,
        ancestorsWithId,
        Function.identity());
  }
}
