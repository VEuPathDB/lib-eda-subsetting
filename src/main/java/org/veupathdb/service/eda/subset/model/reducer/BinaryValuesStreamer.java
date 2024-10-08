package org.veupathdb.service.eda.subset.model.reducer;

import org.gusdb.fgputil.functional.Functions;
import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.filter.MultiFilter;
import org.veupathdb.service.eda.subset.model.filter.SingleValueFilter;
import org.veupathdb.service.eda.subset.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;
import org.veupathdb.service.eda.subset.model.variable.binary.AncestorDeserializer;
import org.veupathdb.service.eda.subset.model.variable.binary.ArrayConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryDeserializer;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryRecordIdValuesConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.ByteArrayConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.ListConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.LongValueConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.RecordIdValuesConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.ValueWithIdDeserializer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BinaryValuesStreamer {
  private static final LongValueConverter LONG_VALUE_CONVERTER = new LongValueConverter();

  private final BinaryFilesManager binaryFilesManager;
  private final ExecutorService fileChannelExecutorService;
  private final ExecutorService deserializerExecutorService;

  public BinaryValuesStreamer(BinaryFilesManager binaryFilesManager,
                              ExecutorService fileChannelExecutorService,
                              ExecutorService deserializerExecutorService) {
    this.binaryFilesManager = binaryFilesManager;
    this.fileChannelExecutorService = fileChannelExecutorService;
    this.deserializerExecutorService = deserializerExecutorService;
  }

  /**
   * Stream entity IDs from a binary file based on the filter passed in. Passes the predicate associated with the filter
   * to the {@link FilteredValueIterator} which applies the predicate to each variable value and streams entity IDs
   * whose values match the predicate.
   *
   * @param filter Filter for which to create a {@link FilteredValueIterator}.
   * @param <V>    Type of variable {@link VariableWithValues}
   * @param <T>    Type of value associated with {@link V}
   * @throws IOException if there is a failure to open the binary file.
   */
  public <V, T extends VariableWithValues<V>> CloseableIterator<Long> streamFilteredEntityIdIndexes(
      SingleValueFilter<V, T> filter, Study study) throws IOException {
    BinaryConverter<V> serializer = filter.getVariable().getBinaryConverter();
    return new FilteredValueIterator<>(
        binaryFilesManager.getVariableFile(study,
            filter.getEntity(),
            filter.getVariable(),
            BinaryFilesManager.Operation.READ),
        filter.getPredicate(),
        new ValueWithIdDeserializer<>(serializer),
        VariableValueIdPair::getIdIndex,
        fileChannelExecutorService,
        deserializerExecutorService);
  }

  /**
   * Stream entity IDs pulled based on the union or intersection of multiple streams of ID indexes from different
   * string set filters.
   *
   * @param filter Filter to apply to variable values.
   * @param study  Study that the request is applicable to.
   */
  public CloseableIterator<Long> streamMultiFilteredEntityIdIndexes(MultiFilter filter, Study study) {
    List<CloseableIterator<Long>> idStreams = filter.getSubFilters().stream()
      .map(Functions.fSwallow(subFilter -> streamFilteredEntityIdIndexes(filter.getFilter(subFilter), study)))
      .toList();
    if (idStreams.size() == 1) {
      return new StreamDeduper(idStreams.getFirst());
    }
    if (filter.getOperation() == MultiFilter.MultiFilterOperation.UNION) {
      return new StreamUnionMerger(idStreams);
    } else { // operation == MultiFilter.MultiFilterOperation.INTERSECT
      return new StreamIntersectMerger(idStreams);
    }
  }

  public <V> CloseableIterator<VariableValueIdPair<byte[]>> streamIdValueBinaryPairs(
      Study study,
      VariableWithValues<V> variable,
      TabularReportConfig reportConfig) throws IOException {
    Function<byte[], byte[]> binaryFormatter = variable.getRawUtf8BinaryFormatter(reportConfig);
    return streamIdValueBinaryPairs(study, variable, reportConfig, binaryFormatter);
  }

  public <V> CloseableIterator<VariableValueIdPair<String>> streamUnformattedIdValueBinaryPairs(
      Study study,
      VariableWithValues<V> variable) throws IOException {
    final TabularReportConfig tabularReportConfig = new TabularReportConfig();
    BinaryConverter<byte[]> serializer = new ByteArrayConverter(variable.getStringConverter().numBytes());
    final BinaryDeserializer<VariableValueIdPair<byte[]>> deserializer = new ValueWithIdDeserializer<>(serializer);
    return new FilteredValueIterator<>(
        binaryFilesManager.getUtf8VariableFile(study,
            variable.getEntity(),
            variable,
            BinaryFilesManager.Operation.READ),
        x -> true, // Always return true, extract all ID index pairs and variable values.
        deserializer,
        pair -> new VariableValueIdPair<>(pair.getIdIndex(),
            variable.getRawUtf8StringFormatter(tabularReportConfig).apply(pair.getValue())),
        fileChannelExecutorService,
        deserializerExecutorService); // Provide a stream of entire VariableValueIdPair objects.
  }


  /**
   * Streams tuples of all entity ID indexes and the string version of variable values associate with the variable
   * passed in.
   *
   * @param study    The study that the variable belongs to. Used to locate the binary file.
   * @param variable The variable whose values are requested.
   * @param <V>      The type of the variable values.
   * @return An iterator  all {@link VariableValueIdPair}s containing all ID indexes and associated variable values.
   * @throws IOException if there is a failure to open the binary file.
   */
  public <V> FilteredValueIterator<byte[], VariableValueIdPair<byte[]>> streamIdValueBinaryPairs(
      Study study,
      VariableWithValues<V> variable,
      TabularReportConfig reportConfig,
      Function<byte[], byte[]> binaryFormatter) throws IOException {
    BinaryConverter<byte[]> serializer = new ByteArrayConverter(variable.getStringConverter().numBytes());
    final BinaryDeserializer<VariableValueIdPair<byte[]>> deserializer = new ValueWithIdDeserializer<>(serializer);

    // Function to translate raw padded utf-8 bytes with formatted value based on report config and variable metadata.
    Function<VariableValueIdPair<byte[]>, VariableValueIdPair<byte[]>> mapper = pair -> {
      pair.setValue(binaryFormatter.apply(pair.getValue()));
      return pair;
    };

    return new FilteredValueIterator<>(
        binaryFilesManager.getUtf8VariableFile(study,
            variable.getEntity(),
            variable,
            BinaryFilesManager.Operation.READ),
        x -> true, // Always return true, extract all ID index pairs and variable values.
        deserializer,
        mapper, // This will map the raw data to utf-8 bytes.
        fileChannelExecutorService,
        deserializerExecutorService); // Provide a stream of entire VariableValueIdPair objects.
  }

  /**
   * @param descendant Entity for which to retrieve ancestors stream.
   * @param study      Study the entity belongs to.
   * @return Stream of ancestor IDs.
   */
  public CloseableIterator<VariableValueIdPair<List<Long>>> streamAncestorIds(Entity descendant,
                                                                              Study study) throws IOException {
    Path path = binaryFilesManager.getAncestorFile(study, descendant, BinaryFilesManager.Operation.READ);
    final ListConverter<Long> listConverter = new ListConverter<>(LONG_VALUE_CONVERTER, descendant.getAncestorEntities().size());
    final ValueWithIdDeserializer<List<Long>> ancestorsWithId = new ValueWithIdDeserializer<>(listConverter);
    return new FilteredValueIterator<>(path,
        x -> true, // Do not apply any filters
        ancestorsWithId,
        Function.identity(),
        fileChannelExecutorService,
        deserializerExecutorService);
  }

  /**
   * @param descendant Entity for which to retrieve ancestors stream.
   * @param study      Study the entity belongs to.
   * @return Stream of ancestor IDs.
   */
  public CloseableIterator<VariableValueIdPair<Long>> streamAncestorIds(Entity descendant,
                                                                        Study study,
                                                                        int colIndex) throws IOException {
    Path path = binaryFilesManager.getAncestorFile(study, descendant, BinaryFilesManager.Operation.READ);
    final ArrayConverter<Long> arrayConverter = new ArrayConverter<>(LONG_VALUE_CONVERTER, descendant.getAncestorEntities().size() + 1, Long.class);
    return new FilteredValueIterator<>(path,
        x -> true, // Do not apply any filters
        new AncestorDeserializer(arrayConverter, colIndex),
        Function.identity(),
        fileChannelExecutorService,
        deserializerExecutorService);
  }

  /**
   * As an iterator, provide a stream of tuples containing:
   * 1. The ID index of an entity
   * 2. The string ID of the entity
   * 3. The string IDs of all ancestors of the entity.
   *
   * @param entity Target entity for data stream.
   * @param study  The study the entity belongs to.
   * @return A pair in which the left is the ID index and the right is a list of ordered string IDs.
   * @throws IOException if there is a failure to open the underlying file.
   */
  public CloseableIterator<VariableValueIdPair<byte[][]>> streamIdMap(Entity entity, Study study) throws IOException {
    Path path = binaryFilesManager.getIdMapFile(study, entity, BinaryFilesManager.Operation.READ);
    BinaryRecordIdValuesConverter converter = constructIdsConverter(study, entity);
    return new FilteredValueIterator<>(path,
        x -> true, // Do not apply any filters.
        converter,
        Function.identity(),
        fileChannelExecutorService,
        deserializerExecutorService);
  }

  public CloseableIterator<VariableValueIdPair<List<String>>> streamIdMapAsStrings(Entity entity, Study study) throws IOException {
    Path path = binaryFilesManager.getIdMapFile(study, entity, BinaryFilesManager.Operation.READ);
    List<Integer> bytesReservedForAncestors = binaryFilesManager.getBytesReservedForAncestry(study, entity);
    Integer bytesReservedForId = binaryFilesManager.getBytesReservedForEntity(study, entity);
    RecordIdValuesConverter converter = new RecordIdValuesConverter(bytesReservedForAncestors, bytesReservedForId);
    return new FilteredValueIterator<>(path,
        x -> true, // Do not apply any filters.
        converter,
        x -> x,
        fileChannelExecutorService,
        deserializerExecutorService);
  }

  public CloseableIterator<Long> streamUnfilteredEntityIdIndexes(Study study, Entity entity) throws IOException {
    BinaryRecordIdValuesConverter converter = constructIdsConverter(study, entity);
    return new FilteredValueIterator<>(
        binaryFilesManager.getIdMapFile(study,
            entity,
            BinaryFilesManager.Operation.READ),
        x -> true,
        converter,
        VariableValueIdPair::getIdIndex,
        fileChannelExecutorService,
        deserializerExecutorService);
  }

  private BinaryRecordIdValuesConverter constructIdsConverter(Study study, Entity entity) {
    List<Integer> bytesReservedForAncestors = binaryFilesManager.getBytesReservedForAncestry(study, entity);
    Integer bytesReservedForId = binaryFilesManager.getBytesReservedForEntity(study, entity);
    return new BinaryRecordIdValuesConverter(bytesReservedForAncestors, bytesReservedForId);
  }
}
