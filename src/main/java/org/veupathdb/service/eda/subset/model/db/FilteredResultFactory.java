package org.veupathdb.service.eda.subset.model.db;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;

import static org.gusdb.fgputil.FormatUtil.NL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.ListBuilder;
import org.gusdb.fgputil.Tuples.TwoTuple;
import org.gusdb.fgputil.db.platform.DBPlatform;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.db.runner.SingleLongResultSetHandler;
import org.gusdb.fgputil.db.stream.ResultSetIterator;
import org.gusdb.fgputil.db.stream.ResultSets;
import org.gusdb.fgputil.functional.TreeNode;
import org.gusdb.fgputil.iterator.CloseableIterator;
import org.gusdb.fgputil.iterator.GroupingIterator;
import org.gusdb.fgputil.iterator.IteratorUtil;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.reducer.FormattedTabularRecordStreamer;
import org.veupathdb.service.eda.subset.model.reducer.UnformattedTabularRecordStreamer;
import org.veupathdb.service.eda.subset.model.reducer.ValueStream;
import org.veupathdb.service.eda.subset.model.reducer.ancestor.EntityIdIndexIteratorConverter;
import org.veupathdb.service.eda.subset.model.reducer.formatter.MultiValueFormatter;
import org.veupathdb.service.eda.subset.model.reducer.formatter.SingleValueFormatter;
import org.veupathdb.service.eda.subset.model.reducer.formatter.TabularValueFormatter;
import org.veupathdb.service.eda.subset.model.tabular.SortSpecEntry;
import org.veupathdb.service.eda.subset.model.tabular.TabularHeaderFormat;
import org.veupathdb.service.eda.subset.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.subset.model.filter.Filter;
import org.veupathdb.service.eda.subset.model.tabular.TabularResponses;
import org.veupathdb.service.eda.subset.model.tabular.TabularResponses.FormatterFactory;
import org.veupathdb.service.eda.subset.model.tabular.TabularResponses.ResultConsumer;
import org.veupathdb.service.eda.subset.model.variable.Variable;
import org.veupathdb.service.eda.subset.model.variable.VariableType;
import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;
import org.veupathdb.service.eda.subset.model.reducer.BinaryValuesStreamer;
import org.veupathdb.service.eda.subset.model.reducer.DataFlowNodeContents;
import org.veupathdb.service.eda.subset.model.reducer.DataFlowTreeFactory;
import org.veupathdb.service.eda.subset.model.reducer.DataFlowTreeReducer;

import static org.gusdb.fgputil.iterator.IteratorUtil.toIterable;

/**
 * A class to perform subsetting operations on a study entity
 *
 * @author Steve
 */
public class FilteredResultFactory {

  private static final Logger LOG = LogManager.getLogger(FilteredResultFactory.class);

  private static final int FETCH_SIZE_FOR_TABULAR_QUERIES = 2000;

  private static final String COUNT_COLUMN_NAME = "count";

  /**
   * Writes to the passed output stream a "tabular" result.  Exact format depends on the passed
   * responseType (JSON string[][] vs true tabular). Each row is a record containing
   * the primary key columns and requested variables of the specified entity.
   *
   * @param dbInstance      DB to run against
   * @param study           study context
   * @param outputEntity    entity type to return
   * @param outputVariables variables requested
   * @param filters         filters to apply to create a subset of records
   * @param reportConfig    configuration of this report
   * @param formatter       object that will write response
   * @param outputStream    stream to which report should be written
   */
  public static void produceTabularSubset(DatabaseInstance dbInstance, String appDbSchema, Study study, Entity outputEntity,
                                          List<VariableWithValues<?>> outputVariables, List<Filter> filters,
                                          TabularReportConfig reportConfig, FormatterFactory formatter,
                                          OutputStream outputStream) {

    // produce output; result consumer will format result and write to passed output stream
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
      produceTabularSubset(dbInstance, appDbSchema, study, outputEntity, outputVariables, filters, reportConfig, formatter.getFormatter(writer));
      writer.flush();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> CloseableIterator<T> toCloseableIterator(Iterator<T> iterator,  AutoCloseable... closeables) {
    return new CloseableIterator<>() {
      @Override
      public void close() {
        Arrays.stream(closeables).forEach(closeable -> {
          try {
            closeable.close();
          } catch (Exception e) {
            LOG.warn("Failed to close closeable.", e);
          }
        });
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        return iterator.next();
      }
    };
  }

  /**
   * Returns a closeable Iterator that has tabular subset records. The returned maps contain keys with entity/ancestor
   * column names and variable column names mapping to their respective values.
   * <p>
   * The keys returned use variable dot notation.
   */
  public static CloseableIterator<Map<String, String>> tabularSubsetIterator(Study study, Entity outputEntity,
                                                                             List<VariableWithValues<?>> outputVariables,
                                                                             List<Filter> filters,
                                                                             BinaryValuesStreamer binaryValuesStreamer,
                                                                             boolean fileBasedEnabled, DatabaseInstance dbInstance,
                                                                             String appDbSchema) {
    if (fileBasedEnabled) {
      return fileTabularSubsetIterator(study, outputEntity, outputVariables, filters, binaryValuesStreamer);
    } else {
      return sqlTabularSubsetIterator(dbInstance, appDbSchema, study, outputEntity, outputVariables, filters);
    }
  }

  /**
   * File-based implementation of tabular record iterator.
   */
  private static CloseableIterator<Map<String, String>> fileTabularSubsetIterator(Study study, Entity outputEntity,
                                                                                  List<VariableWithValues<?>> outputVariables, List<Filter> filters,
                                                                                  BinaryValuesStreamer binaryValuesStreamer) {
    final DataFlowTreeFactory dataFlowTreeFactory = new DataFlowTreeFactory();
    final EntityIdIndexIteratorConverter idIndexEntityConverter = new EntityIdIndexIteratorConverter(binaryValuesStreamer);
    final TreeNode<Entity> prunedEntityTree = pruneTree(study.getEntityTree(), filters, outputEntity);
    final TreeNode<DataFlowNodeContents> dataFlowTree = dataFlowTreeFactory.create(
      prunedEntityTree, outputEntity, filters, outputVariables, study);

    List<String> outputColumns = getTabularOutputColumns(outputEntity, outputVariables);

    final DataFlowTreeReducer driver = new DataFlowTreeReducer(idIndexEntityConverter, binaryValuesStreamer);
    try {
      // Retrieve stream of ID Indexes with all filters applied by traversing the map reduce data flow tree.
      final CloseableIterator<Long> idIndexStream = driver.reduce(dataFlowTree);

      // Open streams of output variables and ancestors identifiers used to decorate ID index stream to produce tabular records.
      List<ValueStream<String>> outputVarStreams = new ArrayList<>();
      for (VariableWithValues<?> outputVar : outputVariables) {
        TabularValueFormatter valFormatter = outputVar.getIsMultiValued() ? new MultiValueFormatter() : new SingleValueFormatter();
        // ValueStream should be UTF-8 byte arrays.
        ValueStream<String> valStream = new ValueStream<>(
          binaryValuesStreamer.streamUnformattedIdValueBinaryPairs(study, outputVar), valFormatter);
        outputVarStreams.add(valStream);
      }

      final CloseableIterator<VariableValueIdPair<List<String>>> idsMapStream = binaryValuesStreamer.streamIdMapAsStrings(outputEntity, study);

      return new UnformattedTabularRecordStreamer(
        outputVarStreams,
        idIndexStream,
        idsMapStream,
        outputColumns
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Oracle-based implementation of tabular record iterator.
   */
  private static CloseableIterator<Map<String, String>> sqlTabularSubsetIterator(DatabaseInstance dbInstance, String appDbSchema, Study study, Entity outputEntity,
                                                                                 List<VariableWithValues<?>> outputVariables, List<Filter> filters) {
    TreeNode<Entity> prunedEntityTree = pruneTree(study.getEntityTree(), filters, outputEntity);

    String sql = generateTabularSqlForTallRows(dbInstance.getPlatform(), appDbSchema, outputVariables, outputEntity, filters, prunedEntityTree);

    // gather the output columns; these will be used for the standard header and to look up DB column values
    List<String> outputColumns = getTabularOutputColumns(outputEntity, outputVariables);

    try {
      Connection connection = dbInstance.getDataSource().getConnection();
      return new SQLRunner(connection, sql, "Produce tabular subset").setNotResponsibleForClosing().executeQuery(rs -> {
        try {
          return toCloseableIterator(
            iteratorFromWideResult(
              convertTallRowsResultSet(rs, outputEntity), outputEntity, outputColumns), rs, connection);
        }
        catch (Exception e) {
          LOG.warn("Exception, ", e);

          // Close the connection if we throw an exception initializing the stream.
          // It's not in a try-with-resources, because in the happy-path, the upstream caller is responsible for closing resources.
          connection.close();
          throw new RuntimeException("Unable to write result", e);
        }
      }, FETCH_SIZE_FOR_TABULAR_QUERIES);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static void produceTabularSubset(DatabaseInstance dbInstance, String appDbSchema, Study study, Entity outputEntity,
                                          List<VariableWithValues<?>> outputVariables, List<Filter> filters,
                                          TabularReportConfig reportConfig, ResultConsumer resultConsumer) {

    TreeNode<Entity> prunedEntityTree = pruneTree(study.getEntityTree(), filters, outputEntity);

    String sql = reportConfig.requiresSorting()
      ? generateTabularSqlForWideRows(dbInstance.getPlatform(), appDbSchema, outputVariables, outputEntity, filters, reportConfig, prunedEntityTree)
      : generateTabularSqlForTallRows(dbInstance.getPlatform(), appDbSchema, outputVariables, outputEntity, filters, prunedEntityTree);

    // gather the output columns; these will be used for the standard header and to look up DB column values
    List<String> outputColumns = getColumns(outputEntity, outputVariables, Entity::getPKColName, Variable::getId);

    // check if header should contain pretty display values
    boolean usePrettyHeader = reportConfig.getHeaderFormat() == TabularHeaderFormat.DISPLAY;

    // create a date formatter based on config
    boolean trimTimeFromDateVars = reportConfig.getTrimTimeFromDateVars();

    new SQLRunner(dbInstance.getDataSource(), sql, "Produce tabular subset").executeQuery(rs -> {
      try {
        resultConsumer.begin();

        // write header row
        resultConsumer.consumeRow(usePrettyHeader ? getTabularPrettyHeaders(outputEntity, outputVariables) : outputColumns);

        if (reportConfig.requiresSorting())
          writeWideRowsFromWideResult(rs, resultConsumer, outputColumns, outputEntity, trimTimeFromDateVars);
        else
          writeWideRowsFromTallResult(convertTallRowsResultSet(rs, outputEntity), resultConsumer, outputColumns, outputEntity, trimTimeFromDateVars);

        // close out the response and flush
        resultConsumer.end();
        return null;
      }
      catch (Exception e) {
        throw new RuntimeException("Unable to write result", e);
      }
    }, FETCH_SIZE_FOR_TABULAR_QUERIES);
  }

  /**
   * Writes to the passed output stream a "tabular" result.  Exact format depends on the passed
   * responseType (JSON string[][] vs true tabular). Each row is a record containing
   * the primary key columns and requested variables of the specified entity.
   *
   * @param study           study context
   * @param outputEntity    entity type to return
   * @param outputVariables variables requested
   * @param filters         filters to apply to create a subset of records
   */
  public static void produceTabularSubsetFromFile(Study study, Entity outputEntity,
                                                  List<VariableWithValues<?>> outputVariables, List<Filter> filters,
                                                  TabularResponses.BinaryFormatterFactory formatter, TabularReportConfig reportConfig,
                                                  OutputStream outputStream,
                                                  BinaryValuesStreamer binaryValuesStreamer) {
    final DataFlowTreeFactory dataFlowTreeFactory = new DataFlowTreeFactory();
    final EntityIdIndexIteratorConverter idIndexEntityConverter = new EntityIdIndexIteratorConverter(binaryValuesStreamer);
    final TreeNode<Entity> prunedEntityTree = pruneTree(study.getEntityTree(), filters, outputEntity);
    final TreeNode<DataFlowNodeContents> dataFlowTree = dataFlowTreeFactory.create(
      prunedEntityTree, outputEntity, filters, outputVariables, study);

    // check if header should contain pretty display values
    boolean usePrettyHeader = reportConfig.getHeaderFormat() == TabularHeaderFormat.DISPLAY;

    // gather the output columns; these will be used for the standard header and to look up DB column values
    List<String> outputColumns = getColumns(outputEntity, outputVariables, Entity::getPKColName, Variable::getId);

    final DataFlowTreeReducer driver = new DataFlowTreeReducer(idIndexEntityConverter, binaryValuesStreamer);
    try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream)) {
      final TabularResponses.BinaryResultConsumer resultConsumer = formatter.getFormatter(bufferedOutputStream);

      resultConsumer.begin();
      resultConsumer.consumeRow(usePrettyHeader ? getTabularPrettyHeaders(outputEntity, outputVariables).stream()
        .map(s -> s.getBytes(StandardCharsets.UTF_8))
        .toArray(byte[][]::new):
        outputColumns.stream()
          .map(s -> s.getBytes(StandardCharsets.UTF_8))
          .toArray(byte[][]::new));

      // Retrieve stream of ID Indexes with all filters applied by traversing the map reduce data flow tree.
      final CloseableIterator<Long> idIndexStream = driver.reduce(dataFlowTree);

      // Open streams of output variables and ancestors identifiers used to decorate ID index stream to produce tabular records.
      List<ValueStream<byte[]>> outputVarStreams = new ArrayList<>();
      for (VariableWithValues<?> outputVar: outputVariables) {
        TabularValueFormatter valFormatter = outputVar.getIsMultiValued() ? new MultiValueFormatter() : new SingleValueFormatter();
        // ValueStream should be UTF-8 byte arrays.
        ValueStream<byte[]> valStream = new ValueStream<>(
          binaryValuesStreamer.streamIdValueBinaryPairs(study, outputVar, reportConfig), valFormatter);
        outputVarStreams.add(valStream);
      }

      final CloseableIterator<VariableValueIdPair<byte[][]>> idsMapStream = binaryValuesStreamer.streamIdMap(outputEntity, study);

      try (final FormattedTabularRecordStreamer resultStreamer = new FormattedTabularRecordStreamer(
        outputVarStreams,
        idIndexStream,
        idsMapStream
      )) {
        long rowsConsumed = 0L;
        long rowsSkipped = 0L;
        while (resultStreamer.hasNext()) {
          if (rowsSkipped >= reportConfig.getOffset()) {
            resultConsumer.consumeRow(resultStreamer.next());
            rowsConsumed++;
          } else {
            resultStreamer.next();
            rowsSkipped++;
          }
          if (reportConfig.getNumRows().isPresent() && rowsConsumed >= reportConfig.getNumRows().get()) {
            break;
          }
        }
        resultConsumer.end();
        LOG.info("Completed processing file-based subsetting request");
      } catch (Exception e) {
        throw new RuntimeException("Failed to write result", e);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to write result", e);
    }
  }


  static <T extends Variable> List<String> getTabularPrettyHeaders(Entity outputEntity, List<T> outputVariables) {
    return getColumns(outputEntity, outputVariables, Entity::getDownloadPkColHeader, Variable::getDownloadColHeader);
  }

  static <T extends Variable> List<String> getTabularOutputColumns(Entity outputEntity, List<T> outputVariables) {
    return getTabularOutputColumns(outputEntity, outputVariables, var -> var.getEntityId() + "." + var.getId());
  }

  static <T extends Variable> List<String> getTabularOutputColumns(Entity outputEntity, List<T> outputVariables, Function<Variable, String> varMapper) {
    return getColumns(outputEntity, outputVariables, e -> e.getId() + "." + e.getPKColName(), varMapper);
  }


  private static  <T extends Variable> List<String> getColumns(Entity outputEntity, List<T> outputVariables,
                                                               Function<Entity, String> pkMapper, Function<Variable,String> varMapper) {
    List<String> outputColumns = new ArrayList<>();
    outputColumns.add(pkMapper.apply(outputEntity));
    outputColumns.addAll(outputEntity.getAncestorEntities().stream().map(pkMapper).toList());
    outputColumns.addAll(outputVariables.stream().map(varMapper).toList());
    return outputColumns;
  }

  /**
   * @return an iterator of maps, each representing a single row in the tall table
   */
  private static Iterator<Map<String, String>> convertTallRowsResultSet(ResultSet rs, Entity outputEntity) {
    return new ResultSetIterator<>(rs, row -> Optional.of(TallRowConversionUtils.resultSetToTallRowMap(outputEntity, rs)));
  }

  private static List<String> getDateVarNames(Entity entity, List<String> desiredVarNames) {
    // create a list of date var names in this result; they may be trimmed
    return desiredVarNames.stream()
      // find the var by each name (null for ID names, which won't be found)
      .map(name -> entity.getVariable(name).orElse(null))
      // filter out ID names and non-dates
      .filter(var -> var != null && ((VariableWithValues<?>)var).getType() == VariableType.DATE)
      // convert back to the ID/name of the variable
      .map(Variable::getId)
      // collect into a list
      .toList();
  }

  static Iterator<Map<String, String>> iteratorFromWideResult(Iterator<Map<String, String>> tallRowsIterator,
                                                              Entity outputEntity, List<String> headers) {
    // an iterator of lists of maps, each list being the rows of the tall table returned for a single entity id
    String pkCol = outputEntity.getPKColName();
    Iterator<List<Map<String, String>>> groupedTallRowsIterator = new GroupingIterator<>(
      tallRowsIterator, (row1, row2) -> row1.get(pkCol).equals(row2.get(pkCol)));

    // iterate through groups and format into strings to be written to stream
    return IteratorUtil.transform(
      groupedTallRowsIterator, group -> TallRowConversionUtils.getTallToWideFunction(outputEntity, headers,
        Entity::getPKColDotNotation, Variable::getDotNotation).apply(group));
  }


  static void writeWideRowsFromTallResult(Iterator<Map<String, String>> tallRowsIterator,
                                          ResultConsumer resultConsumer, List<String> outputColumns,
                                          Entity outputEntity, boolean trimTimeFromDateVars) throws IOException {

    // an iterator of lists of maps, each list being the rows of the tall table returned for a single entity id
    String pkCol = outputEntity.getPKColName();
    Iterator<List<Map<String, String>>> groupedTallRowsIterator = new GroupingIterator<>(
      tallRowsIterator, (row1, row2) -> row1.get(pkCol).equals(row2.get(pkCol)));

    // iterate through groups and format into strings to be written to stream
    List<String> dateVars = getDateVarNames(outputEntity, outputColumns);
    for (List<Map<String, String>> group : toIterable(groupedTallRowsIterator)) {
      Map<String, String> wideRowMap = TallRowConversionUtils.getTallToWideFunction(outputEntity).apply(group);

      // build list of row values
      List<String> wideRow = new ArrayList<>(outputColumns.size());
      for (String colName : outputColumns) {
        // look up column value in the map
        String value = wideRowMap.get(colName);
        // convert null values to empty strings
        if (value == null) value = "";
        // trim dates if necessary
        if (trimTimeFromDateVars && dateVars.contains(colName) && value.length() > 10) value = value.substring(0,10);
        // add to row
        wideRow.add(value);
      }
      resultConsumer.consumeRow(wideRow);
    }
  }

  static void writeWideRowsFromWideResult(ResultSet rs, ResultConsumer resultConsumer, List<String> outputColumns, Entity outputEntity,
                                          boolean trimTimeFromDateVars) throws IOException, SQLException {

    // iterate through groups and format into strings to be written to stream
    List<String> dateVars = getDateVarNames(outputEntity, outputColumns);

    // iterate over wide result rows, converting dates if necessary
    while (rs.next()) {
      List<String> wideRow = new ArrayList<>(outputColumns.size());
      for (String colName : outputColumns) {
        // read raw value from result set, defaulting to empty string
        String value = ResultSetUtils.getRsOptionalString(rs, colName, "");
        // data vars need extra formatting for ISO format compliance; also, trim dates if necessary
        if (dateVars.contains(colName) && value.length() > 10) {
          if (trimTimeFromDateVars)
            value = value.substring(0,10);
          else
            value = value.replace(' ','T');
        }
        wideRow.add(value);
      }
      resultConsumer.consumeRow(wideRow);
    }
  }

  /**
   * NOTE! This stream MUST be closed by the caller once the stream has been processed.
   * The easiest way to do this is with a try-with-resources around this method's call.
   *
   * @return stream of distribution tuples
   */
  public static Stream<TwoTuple<String, Long>> produceVariableDistribution(
    DatabaseInstance dbInstance, String appDbSchema, TreeNode<Entity> prunedEntityTree, Entity outputEntity,
    VariableWithValues<?> distributionVariable, List<Filter> filters) {
    String sql = generateDistributionSql(dbInstance.getPlatform(), appDbSchema, outputEntity, distributionVariable, filters, prunedEntityTree);
    return ResultSets.openStream(dbInstance.getDataSource(), sql, "Produce variable distribution", row -> Optional.of(
      new TwoTuple<>(distributionVariable.getType().convertRowValueToStringValue(row), row.getLong(COUNT_COLUMN_NAME))));
  }

  public static long getVariableCount(
    DataSource datasource, String appDbSchema, TreeNode<Entity> prunedEntityTree, Entity outputEntity,
    Variable distributionVariable, List<Filter> filters) {
    String sql = generateVariableCountSql(appDbSchema, outputEntity, distributionVariable, filters, prunedEntityTree);
    return new SQLRunner(datasource, sql, "Get variable count for distribution").executeQuery(new SingleLongResultSetHandler())
      .orElseThrow(() -> new RuntimeException("Could not retrieve variable count"));
  }

  public static long getEntityCount(
    DataSource datasource, String appDbSchema, TreeNode<Entity> prunedEntityTree, Entity targetEntity, List<Filter> filters) {
    String sql = generateEntityCountSql(appDbSchema, targetEntity, filters, prunedEntityTree);
    return new SQLRunner(datasource, sql, "Get entity count").executeQuery(new SingleLongResultSetHandler())
      .orElseThrow(() -> new RuntimeException("Could not retrieve variable count"));
  }

  public static long getEntityCount(TreeNode<Entity> prunedEntityTree,
                                    Entity targetEntity,
                                    List<Filter> filters,
                                    BinaryValuesStreamer binaryValuesStreamer,
                                    Study study) {
    final DataFlowTreeFactory dataFlowTreeFactory = new DataFlowTreeFactory();
    final EntityIdIndexIteratorConverter idIndexEntityConverter = new EntityIdIndexIteratorConverter(binaryValuesStreamer);
    final TreeNode<DataFlowNodeContents> dataFlowTree = dataFlowTreeFactory.create(
      prunedEntityTree, targetEntity, filters, List.of(), study);

    final DataFlowTreeReducer driver = new DataFlowTreeReducer(idIndexEntityConverter, binaryValuesStreamer);
    try (CloseableIterator<Long> outputStream = driver.reduce(dataFlowTree)) {
      int count = 0;
      while (outputStream.hasNext()) {
        outputStream.next();
        count++;
      }
      return count;
    } catch (Exception e) {
      throw new RuntimeException("Error when producing count for entity " + targetEntity.getId() + " for study " + study.getStudyId(), e);
    }
  }

  /**
   * Prune tree to include only active nodes, based on filters and output entity
   */
  public static TreeNode<Entity> pruneTree(TreeNode<Entity> tree, List<Filter> filters, Entity outputEntity) {

    List<String> entityIdsInFilters = getEntityIdsInFilters(filters);

    Predicate<Entity> isActive =
      e -> entityIdsInFilters.contains(e.getId()) ||
        e.getId().equals(outputEntity.getId());

    return pruneToActiveAndPivotNodes(tree, isActive);
  }

  static List<String> getEntityIdsInFilters(List<Filter> filters) {
    return filters.stream().map(f -> f.getEntity().getId()).toList();
  }

  /**
   * Generate SQL to produce a tall stream of Entity ID, ancestry IDs, variable ID and values.
   */
  static String generateTabularSqlForTallRows(DBPlatform dbPlatform, String appDbSchema, List<VariableWithValues<?>> outputVariables, Entity outputEntity, List<Filter> filters, TreeNode<Entity> prunedEntityTree) {

    String tallTblAbbrev = "tall";
    String ancestorTblAbbrev = "subset";
    return
      // with clauses create an entity-named filtered result for each relevant entity
      generateFilterWithClauses(appDbSchema, prunedEntityTree, filters) + NL +
        // select
        generateTabularSelectClause(outputEntity, ancestorTblAbbrev) + NL +
        generateTabularFromClause(outputEntity, prunedEntityTree, ancestorTblAbbrev) + NL +
        // left join to attributes table so we always get at least one row per subset
        //   record, even if no data exists for requested vars (or no vars requested).
        // null rows will be handled in the tall-to-wide rows conversion
        generateLeftJoin(dbPlatform, appDbSchema, outputEntity, outputVariables, ancestorTblAbbrev, tallTblAbbrev) + NL +
        generateTabularOrderByClause(outputEntity) + NL;
  }

  /**
   * Generate SQL to produce a multi-column tabular output (the requested variables), for the specified subset, e.g.
   * <pre>
   * WITH
   *   EUPATH_0000609 as (
   *     SELECT Participant_stable_id, Household_stable_id, Sample_stable_id FROM eda.Ancestors_GEMSCC0003_1_Sample
   *   ),
   *   subset AS (
   *     SELECT EUPATH_0000609.Sample_stable_id
   *     FROM EUPATH_0000609
   *   ),
   *   wide_tabular AS (
   *     select rownum as r, wt.*
   *     from (
   *       select Sample_stable_id, Participant_stable_id, Household_stable_id,
   *         json_query(atts, '$.EUPATH_0000711') as EUPATH_0000711,
   *         json_query(atts, '$.OBI_0001619') as OBI_0001619,
   *         ea.stable_id
   *       from eda.entityattributes ea, eda.Ancestors_GEMSCC0003_1_Sample a
   *       where ea.stable_id in (select * from subset)
   *         and ea.stable_id = a.Sample_stable_id
   *       order by Sample_stable_id
   *     ) wt
   *   )
   * select Sample_stable_id, Participant_stable_id, Household_stable_id, EUPATH_0000711, OBI_0001619
   * from wide_tabular
   * where r > 2 and r <= 8
   * order by Sample_stable_id;
   * </pre>
   */
  static String generateTabularSqlForWideRows(DBPlatform dbPlatform, String appDbSchema, List<VariableWithValues<?>> outputVariables, Entity outputEntity, List<Filter> filters,
                                              TabularReportConfig reportConfig, TreeNode<Entity> prunedEntityTree) {
    String wideTabularWithClauseName = "wide_tabular";
    String subsetWithClauseName = "subset";
    String rowColName = "r";

    //
    // build up WITH clauses
    //
    String wideTabularInnerStmt = generateRawWideTabularInnerStmt(appDbSchema, outputEntity, outputVariables, subsetWithClauseName, reportConfig);
    String wideTabularStmt = generateRawWideTabularOuterStmt(wideTabularInnerStmt, dbPlatform);
    String subsetSelectClause = generateSubsetSelectClause(prunedEntityTree, outputEntity, false);

    List<String> withClausesList = prunedEntityTree.flatten().stream()
      .map(e -> generateFilterWithClause(appDbSchema, e, filters))
      .collect(Collectors.toCollection(ArrayList::new)); // require mutability for adds below
    withClausesList.add(subsetWithClauseName + " AS (" + NL + subsetSelectClause + ")");
    withClausesList.add(wideTabularWithClauseName + " AS (" + NL + wideTabularStmt + NL + ")");
    String withClauses = joinWithClauses(withClausesList);

    //
    // final select -- quote the variable names for case sensitivity of var names in studies.
    // Note that the quotes are a bit of a hack, it's possible we'd rather enforce case insensitivity at load time.
    //
    List<String> outputCols = getColumns(outputEntity, outputVariables, Entity::getPKColName, var -> quote(var.getId()));
    return withClauses + NL
      + "select " + String.join(", ", outputCols) + NL
      + "from " + wideTabularWithClauseName + NL
      + reportConfigPagingWhereClause(reportConfig, rowColName) + NL
      + reportConfigOrderByClause(reportConfig.getSorting(), "");
  }

  static String reportConfigPagingWhereClause(TabularReportConfig config, String rowColName) {
    if (config.getOffset() == 0 && config.getNumRows().isEmpty()) {
      return ""; // no paging
    }

    long start = config.getOffset();
    String whereClause = "where " + rowColName + " > " + start;
    if (config.getNumRows().isPresent()) {
      whereClause += " and " + rowColName + " <= " + (start + config.getNumRows().get());
    }
    return whereClause;
  }

  static String reportConfigOrderByClause(List<SortSpecEntry> config, String indent) {
    return config.isEmpty() ? "" :
      config.stream()
        .map(entry -> quote(entry.getKey()) + " " + entry.getDirection())
        .collect(Collectors.joining(", ", indent + " order by ", " "));
  }

  /*
   *       select json_query(atts, '$.EUPATH_0010077') as EUPATH_0010077
      , json_query(atts, '$.CMO_0000289') as CMO_0000289
      , json_query(atts, '$.EUPATH_0015125') as EUPATH_0015125
      , ea.stable_id, rownum as row
      from apidb.entityattributes ea
      where ea.stable_id in (
          select * from subset
        )
   */
  static String generateRawWideTabularInnerStmt(String appDbSchema, Entity outputEntity, List<VariableWithValues<?>> outputVariables,
                                                String subsetWithClauseName, TabularReportConfig reportConfig) {

    List<String> columns = new ArrayList<>();
    columns.add(outputEntity.getPKColName());
    columns.addAll(outputEntity.getAncestorPkColNames());
    for (Variable var : outputVariables) columns.add("\"" + var.getId() + "\"");

    columns.add("ea.stable_id");
    return
      "    select " + String.join(", " + NL + "    ", columns) + NL +
        "    from " + appDbSchema + DB.Tables.Attributes.NAME(outputEntity) + " ea, " + appDbSchema + DB.Tables.Ancestors.NAME(outputEntity) + " a" + NL +
        "    where ea.stable_id in (select * from " + subsetWithClauseName + ")" + NL +
        "    and ea.stable_id = a." + outputEntity.getPKColName() + NL +
        reportConfigOrderByClause(reportConfig.getSorting(), "    ");
  }

  static String generateRawWideTabularOuterStmt(String innerStmt, DBPlatform dbPlatform) {
    return "select " + dbPlatform.getRowNumberColumn() + " as r, wt.*" + NL +
            "  from (" + NL +
            innerStmt + NL +
            "  ) wt";
  }

  static String jsonQuery(String oracleQuery, String postgresQuery) {
    return oracleQuery != null ? oracleQuery : postgresQuery;
  }

  private static String generateLeftJoin(DBPlatform dbPlatform, String appDbSchema, Entity outputEntity, List<VariableWithValues<?>> outputVariables, String ancestorTblAbbrev, String tallTblAbbrev) {
    if (outputVariables.isEmpty()) {
      return " LEFT JOIN ( SELECT " +
        "null as " + DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME + ", " +
        "null as " + DB.Tables.AttributeValue.Columns.STRING_VALUE_COL_NAME + ", " +
        "null as " + DB.Tables.AttributeValue.Columns.DATE_VALUE_COL_NAME + ", " +
        "null as " + DB.Tables.AttributeValue.Columns.NUMBER_VALUE_COL_NAME +
        " " + dbPlatform.getDummyTable() + " ) ON 1 = 1 ";
    }
    String pkColName = outputEntity.getPKColName();
    return " LEFT JOIN (" + NL
      + " SELECT * FROM " + appDbSchema + DB.Tables.AttributeValue.NAME(outputEntity) + " " + NL
      + generateTabularWhereClause(outputVariables, pkColName) + NL
      + " ) " + tallTblAbbrev + NL
      + " ON " + ancestorTblAbbrev + "." + pkColName + " = " + tallTblAbbrev + "." + pkColName;
  }

  /*
   * Generate SQL to produce a multi-column tabular output (the requested variables), for the specified subset.
   */
  static String generateEntityCountSql(String appDbSchema, Entity outputEntity, List<Filter> filters, TreeNode<Entity> prunedEntityTree) {

    return generateFilterWithClauses(appDbSchema, prunedEntityTree, filters) + NL
      + "SELECT count(distinct " + outputEntity.getPKColName() + ") as " + COUNT_COLUMN_NAME + NL
      + "FROM (" + NL
      + generateSubsetSelectClause(prunedEntityTree, outputEntity, false) + NL
      + ") t";
  }

  /**
   * Generate SQL to produce a count of the entities that have a value for a variable, for the specified subset.
   */
  static String generateVariableCountSql(String appDbSchema, Entity outputEntity, Variable variable, List<Filter> filters, TreeNode<Entity> prunedEntityTree) {
    return generateFilterWithClauses(appDbSchema, prunedEntityTree, filters) + NL
      + generateVariableCountSelectClause(variable) + NL
      + generateDistributionFromClause(appDbSchema, outputEntity) + NL
      + generateDistributionWhereClause(variable) + NL
      + generateSubsetInClause(prunedEntityTree, outputEntity, DB.Tables.AttributeValue.NAME(outputEntity));

  }

  static String generateFilterWithClauses(String appDbSchema, TreeNode<Entity> prunedEntityTree, List<Filter> filters) {
    List<String> withClauses = prunedEntityTree.flatten().stream().map(e -> generateFilterWithClause(appDbSchema, e, filters)).toList();
    return joinWithClauses(withClauses);
  }

  static String joinWithClauses(List<String> withClauses) {
    return "WITH" + NL
      + String.join("," + NL, withClauses);
  }

  /*
   * Get a with clause for this entity.  If the filters don't include any from this entity,
   * then the with clause will just select * from the entity's ancestor table
   */
  static String generateFilterWithClause(String appDbSchema, Entity entity, List<Filter> filters) {

    List<String> selectColsList = new ArrayList<>(entity.getAncestorPkColNames());
    selectColsList.add(entity.getPKColName());
    String selectCols = String.join(", ", selectColsList);

    // default WITH body assumes no filters. we use the ancestor table because it is small
    String withBody = "  SELECT " + selectCols + " FROM " + appDbSchema + DB.Tables.Ancestors.NAME(entity) + NL;

    List<Filter> filtersOnThisEntity = filters.stream().filter(f -> f.getEntity().getId().equals(entity.getId())).toList();

    if (!filtersOnThisEntity.isEmpty()) {
      List<String> filterSqls = filters.stream().filter(f -> f.getEntity().getId().equals(entity.getId())).map(Filter::getSql).toList();
      withBody = String.join("INTERSECT" + NL, filterSqls);
    }

    return entity.getWithClauseName() + " as (" + NL + withBody + ")";
  }

  static String generateTabularSelectClause(Entity outputEntity, String ancestorTblAbbrev) {
    Set<String> valColNames = Arrays
      .stream(VariableType.values())
      .map(VariableType::getTallTableColumnName)
      .collect(Collectors.toSet());
    return "SELECT " + outputEntity.getAllPksSelectList(ancestorTblAbbrev) + ", " +
      DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME + ", " + String.join(", ", valColNames);
  }

  static String generateVariableCountSelectClause(Variable variable) {
    return "SELECT count(distinct " + variable.getEntity().getPKColName() + ") as " + COUNT_COLUMN_NAME;
  }

  /**
   * Generate SQL to produce a distribution for a single variable, for the specified subset.
   */
  /*
  -- WINNER!  This is the distribution SQL we want (includes null count)
WITH
EUPATH_0000096 as (
  SELECT Household_stable_id, Participant_stable_id FROM apidb.Ancestors_UMSP00001_1_Participant
)
select number_value, count(Participant_stable_id) from (
select subset.*, tall.number_value
from EUPATH_0000096 subset
left join (
  select * from apidb.AttributeValue_UMSP00001_1_Participant
  where attribute_stable_id = 'IAO_0000414'
) tall
on tall.Participant_stable_id = subset.Participant_stable_id
)
group by number_value
order by number_value desc;
   */
  static String generateDistributionSql(DBPlatform dbPlatform, String appDbSchema, Entity outputEntity, VariableWithValues<?> distributionVariable, List<Filter> filters, TreeNode<Entity> prunedEntityTree) {

    String tallTblAbbrev = "tall";
    String ancestorTblAbbrev = "subset";
    String distTblAbbrev = "dist";
    return
      // with clauses create an entity-named filtered result for each relevant entity
      generateFilterWithClauses(appDbSchema, prunedEntityTree, filters) + NL +
        generateDistributionSelectClause(distributionVariable, distTblAbbrev) + NL +
        " FROM ( " + NL +
        generateTabularSelectClause(outputEntity, ancestorTblAbbrev) + NL +
        generateTabularFromClause(outputEntity, prunedEntityTree, ancestorTblAbbrev) + NL +
        // left join to attributes table so we always get at least one row per subset
        //   record, even if no data exists for requested vars (or no vars requested).
        // null rows will be handled in the tall-to-wide rows conversion
        generateLeftJoin(dbPlatform, appDbSchema, outputEntity, List.of(distributionVariable), ancestorTblAbbrev, tallTblAbbrev) + NL +
        " ) dist" + NL +
        generateDistributionGroupByClause(distributionVariable, distTblAbbrev) + NL +
        "ORDER BY " + distTblAbbrev + "." + distributionVariable.getType().getTallTableColumnName() + " ASC";
  }

  static String generateDistributionSelectClause(VariableWithValues<?> distributionVariable, String tableAbbrev) {
    return "SELECT " +
      tableAbbrev + "." + distributionVariable.getType().getTallTableColumnName() +
      ", count(" + tableAbbrev + "." + distributionVariable.getEntity().getPKColName() + ") as " + COUNT_COLUMN_NAME;
  }

  static String generateDistributionFromClause(String appDbSchema, Entity outputEntity) {
    return "FROM " + appDbSchema + DB.Tables.AttributeValue.NAME(outputEntity);
  }

  private static String generateTabularFromClause(Entity outputEntity, TreeNode<Entity> prunedEntityTree, String ancestorTblAbbrev) {
    return " FROM ( " + generateSubsetSelectClause(prunedEntityTree, outputEntity, true) + " ) " + ancestorTblAbbrev;
  }

  static String generateTabularWhereClause(List<VariableWithValues<?>> outputVariables, String entityPkCol) {

    List<String> outputVariableExprs = outputVariables.stream()
      .map(Variable::getId)
      .map(varId -> " " + DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME + " = '" + varId + "'")
      .toList();

    return outputVariableExprs.isEmpty() ? "" :
      " WHERE (" + NL + String.join(" OR" + NL, outputVariableExprs) + NL + ")" + NL;
  }

  static String generateDistributionWhereClause(Variable outputVariable) {
    return "WHERE " + DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME + " = '" + outputVariable.getId() + "'";
  }

  static String generateSubsetInClause(TreeNode<Entity> prunedEntityTree, Entity outputEntity, String tallTblAbbrev) {
    return "AND" + " " + tallTblAbbrev + "." + outputEntity.getPKColName() + " IN (" + NL
      + generateSubsetSelectClause(prunedEntityTree, outputEntity, false) + NL
      + ")";
  }

  static String generateSubsetSelectClause(TreeNode<Entity> prunedEntityTree, Entity outputEntity, boolean returnAncestorIds) {
    return generateJoiningSelectClause(outputEntity, returnAncestorIds) + NL
      + generateJoiningFromClause(prunedEntityTree) + NL
      + generateJoiningJoinsClause(prunedEntityTree);
  }

  static String generateJoiningSelectClause(Entity outputEntity, boolean returnAncestorIds) {
    List<String> returnedCols = ListBuilder.asList(outputEntity.getFullPKColName());
    if (returnAncestorIds) {
      returnedCols.addAll(
        outputEntity.getAncestorPkColNames().stream()
          .map(pk -> outputEntity.getId() + "." + pk)
          .toList());
    }
    return "  SELECT distinct " + String.join(", ", returnedCols);
  }

  static String generateJoiningFromClause(TreeNode<Entity> prunedEntityTree) {
    List<String> fromClauses = prunedEntityTree.flatten().stream().map(Entity::getWithClauseName).collect(Collectors.toList());
    return "  FROM " + String.join(", ", fromClauses);
  }

  static String generateJoiningJoinsClause(TreeNode<Entity> prunedEntityTree) {
    List<String> sqlJoinStrings = new ArrayList<>();
    addSqlJoinStrings(prunedEntityTree, sqlJoinStrings);
    return sqlJoinStrings.isEmpty() ? "" : "  WHERE " + String.join(NL + "  AND ", sqlJoinStrings);
  }

  /*
   * Add to the input list the sql join of a parent entity with each of its children, plus, recursively, its
   * children's sql joins.  (Because the tree might have been pruned, the "parent" is some ancestor)
   */
  static void addSqlJoinStrings(TreeNode<Entity> parent, List<String> sqlJoinStrings) {
    for (TreeNode<Entity> child : parent.getChildNodes()) {
      sqlJoinStrings.add(getSqlJoinString(parent.getContents(), child.getContents()));
      addSqlJoinStrings(child, sqlJoinStrings);
    }
  }

  // this join is formed using the name from the WITH clause, which is the entity name
  static String getSqlJoinString(Entity parentEntity, Entity childEntity) {
    return parentEntity.getWithClauseName() + "." + parentEntity.getPKColName() + " = " +
      childEntity.getWithClauseName() + "." + parentEntity.getPKColName();
  }

  // need to order by the root of the tree first, then by each ID down the branch to the output entity,
  static String generateTabularOrderByClause(Entity outputEntity) {
    List<String> cols = new ArrayList<>();
    // reverse the order of the ancestor pk cols to go root first, parent last
    outputEntity.getAncestorPkColNames().forEach(cols::addFirst);
    // add output entity last
    cols.add(outputEntity.getPKColName());
    return "ORDER BY " + String.join(", ", cols);
  }

  static String generateDistributionGroupByClause(VariableWithValues<?> outputVariable, String tableAbbrev) {
    return "GROUP BY " + tableAbbrev + "." + outputVariable.getType().getTallTableColumnName();
  }

  private static String quote(String s) {
    return "\"" + s + "\"";
  }


  /*
   * PRUNE THE COMPLETE TREE TO JUST THE "ACTIVE" ENTITIES WE WANT FOR OUR JOINS
   *
   * definition: an active entity is one that must be included in the SQL definition: an active subtree is one
   * in which any entities in the subtree are active.
   *
   * this entity is active if any of these apply: 1. it has filters 2. it is the output entity 3. it is
   * neither of the above, but has more than one child that is the root of an active subtree
   *
   * (criterion 3 lets us join elements across connected subtrees)
   *
   * ----X---- | | --I-- I | | | A I A
   *
   * In the picture above the A entities are active and I are inactive. X has two children that are active
   * subtrees. We need to force X to be active so that we can join the lower A entities.
   *
   * So will we now have this:
   *
   * ----A---- | | --I-- I | | | A I A
   *
   * Finally, we want to prune the tree of inactive nodes, so we have the minimal active tree:
   *
   * ----A---- | | A A
   *
   * Now we can ascend the tree and form the concise SQL joins we need
   *
   * Using a concrete example: ----H---- | | --P-- E | | | O S T
   *
   * If O and T are active (have filters or are the output entity), then we ultimately need this join:
   * where O.H_id = H.H_id and T.H_id = H.H_id
   *
   * (The graceful implementation below is courtesy of Ryan)
   */
  private static TreeNode<Entity> pruneToActiveAndPivotNodes(TreeNode<Entity> root, Predicate<Entity> isActive) {
    return root.mapStructure((nodeContents, mappedChildren) -> {
      List<TreeNode<Entity>> activeChildren = mappedChildren.stream()
        .filter(Objects::nonNull) // filter dead branches
        .collect(Collectors.toList());
      return isActive.test(nodeContents) || activeChildren.size() > 1 ?
        // this node is active itself or a pivot node; return with any active children
        new TreeNode<>(nodeContents).addAllChildNodes(activeChildren) :
        // inactive, non-pivot node; return single active child or null
        activeChildren.isEmpty() ? null : activeChildren.getFirst();
    });
  }

}
