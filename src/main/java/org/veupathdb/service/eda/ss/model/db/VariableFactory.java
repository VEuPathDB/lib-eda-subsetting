package org.veupathdb.service.eda.ss.model.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.reducer.BinaryMetadataProvider;
import org.veupathdb.service.eda.ss.model.variable.*;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeGraph.Columns.*;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.*;

public class VariableFactory {

  private static final Logger LOG = LogManager.getLogger(VariableFactory.class);

  private final DataSource _dataSource;
  private final String _appDbSchema;
  private final Optional<BinaryMetadataProvider> _binaryMetadataProvider;
  private final Function<String, Boolean> _shouldAppendMetaForStudy;

  public VariableFactory(DataSource dataSource,
                         String appDbSchema,
                         BinaryMetadataProvider binaryMetadataProvider,
                         Function<String, Boolean> shouldAppendMetaForStudy) {
    _dataSource = dataSource;
    _appDbSchema = appDbSchema;
    _binaryMetadataProvider = Optional.ofNullable(binaryMetadataProvider);
    _shouldAppendMetaForStudy = shouldAppendMetaForStudy;
  }


  List<Variable> loadVariables(String studyAbbrev, Entity entity) {

    String sql = generateStudyVariablesListSql(entity, _appDbSchema);

    Optional<BinaryMetadataProvider> metadataProvider = _shouldAppendMetaForStudy.apply(studyAbbrev)
        ? _binaryMetadataProvider
        : Optional.empty();

    return new SQLRunner(_dataSource, sql, "Get entity variables metadata for: '" + entity.getDisplayName() + "'").executeQuery(rs -> {
      List<Variable> variables = new ArrayList<>();
      while (rs.next()) {
        variables.add(createVariableFromResultSet(rs, entity, metadataProvider));
      }
      return variables;
    });
  }

  static String generateStudyVariablesListSql(Entity entity, String appDbSchema) {
    // This SQL safe from injection because entities declare their own table names (no parameters)
    // TODO: remove hack distinct
    return "SELECT distinct " + String.join(", ", DB.Tables.AttributeGraph.Columns.ALL) + NL
        + "FROM " + appDbSchema + DB.Tables.AttributeGraph.NAME(entity) + NL
        + "ORDER BY " + DB.Tables.AttributeGraph.Columns.VARIABLE_ID_COL_NAME;  // stable ordering supports unit testing
  }

  /**
   * 
   * @param rs Database result set containing variable metadata.
   * @param entity Entity associated with variable.
   * @param binaryMetadataProvider Optional metadata provider to decorate variables with metadata describing how the
   *                               values are encoded as binary. This can be null if we are reading from the database
   *                               exclusively.
   * @return Variable with metadata fully populated.
   * @throws SQLException If there is a failure in executing the query.
   */
  static Variable createVariableFromResultSet(ResultSet rs, Entity entity, Optional<BinaryMetadataProvider> binaryMetadataProvider) throws SQLException {
    Variable.Properties varProps = new Variable.Properties(
        getRsOptionalString(rs, PROVIDER_LABEL_COL_NAME, "No Provider Label available"), // TODO remove hack when in db
        getRsRequiredString(rs, VARIABLE_ID_COL_NAME),
        entity,
        VariableDisplayType.fromString(getRsOptionalString(rs, DISPLAY_TYPE_COL_NAME, VariableDisplayType.DEFAULT.getType())),
        getRsRequiredString(rs, DISPLAY_NAME_COL_NAME),
        getRsOptionalLong(rs, DISPLAY_ORDER_COL_NAME, null),
        getRsOptionalString(rs, VARIABLE_PARENT_ID_COL_NAME, null),
        getRsOptionalString(rs, DEFINITION_COL_NAME, ""),
        parseJsonArrayOfString(rs, HIDE_FROM_COL_NAME));
    // Only set binary properties if binaryMetadataProvider is present.
    LOG.info("Is binaryMetadataProvider present? " + binaryMetadataProvider.isPresent());
    Optional<BinaryProperties> binaryProperties = binaryMetadataProvider.flatMap(provider ->
        provider.getBinaryProperties(entity.getStudyAbbrev(), entity, varProps.id));
    LOG.info("Could find binary properies for study " + entity.getStudyAbbrev() + ", entity " + entity.getId() + "? " + binaryProperties.isPresent());
    return getRsRequiredBoolean(rs, HAS_VALUES_COL_NAME)
        ? createValueVarFromResultSet(rs, varProps, binaryProperties.orElse(null))
        : new VariablesCategory(varProps);
  }

  static Variable createValueVarFromResultSet(ResultSet rs,
                                              Variable.Properties varProps,
                                              BinaryProperties binaryProperties) {
    try {
      VariableWithValues.Properties valueProps = new VariableWithValues.Properties(
          VariableType.fromString(getRsRequiredString(rs, VARIABLE_TYPE_COL_NAME)),
          VariableDataShape.fromString(getRsRequiredString(rs, DATA_SHAPE_COL_NAME)),
          parseJsonArrayOfString(rs, VOCABULARY_COL_NAME),
          rs.getLong(DISTINCT_VALUES_COUNT_COL_NAME),
          rs.getBoolean(IS_TEMPORAL_COL_NAME),
          rs.getBoolean(IS_FEATURED_COL_NAME),
          rs.getBoolean(IS_MERGE_KEY_COL_NAME),
          rs.getBoolean(IS_MULTI_VALUED_COL_NAME),
          rs.getBoolean(IMPUTE_ZERO),
          rs.getBoolean(HAS_STUDY_DEPENDENT_VOCABULARY),
          rs.getString(VARIABLE_SPEC_TO_IMPUTE_ZEROES_FOR));

      switch(valueProps.type) {

        case NUMBER: return
            new FloatingPointVariable(varProps, valueProps, createFloatDistributionConfig(rs, true), createFloatProperties(rs, true));

        case LONGITUDE: return
            new LongitudeVariable(varProps, valueProps, new LongitudeVariable.Properties(
                getRsOptionalLong(rs, PRECISION_COL_NAME, 1L)
            ));

        case INTEGER: return
            new IntegerVariable(varProps, valueProps, createIntegerDistributionConfig(rs, true), createIntegerProperties(rs));

        case DATE: return
            new DateVariable(varProps, valueProps, createDateDistributionConfig(valueProps.dataShape, rs, true));

        case STRING:
          return new StringVariable(varProps, valueProps, (StringVariable.StringBinaryProperties) binaryProperties);

        default: throw new RuntimeException("Entity:  " + varProps.entity.getId() +
            " variable: " + varProps.id + " has unrecognized type " + valueProps.type);
      }
    }
    catch (SQLException e) {
      throw new RuntimeException("Entity:  " + varProps.entity.getId() + " variable: " + varProps.id, e);
    }
  }

  public static IntegerVariable.Properties createIntegerProperties(ResultSet rs) throws SQLException {
    return new IntegerVariable.Properties(
        getRsOptionalString(rs, UNITS_COL_NAME, "")
    );
  }

  public static FloatingPointVariable.Properties createFloatProperties(ResultSet rs, boolean sqlContainsScale) throws SQLException {
    return new FloatingPointVariable.Properties(
        getRsOptionalString(rs, UNITS_COL_NAME, ""),
        getRsOptionalLong(rs, PRECISION_COL_NAME, 1L),
        VariableScale.findByValue(sqlContainsScale ? getRsOptionalString(rs, SCALE_COL_NAME, null) : null)
    );
  }

  public static DateDistributionConfig createDateDistributionConfig(
      VariableDataShape dataShape, ResultSet rs, boolean includeBinInfo) throws SQLException {
    return new DateDistributionConfig(includeBinInfo,
        dataShape,
        getRsOptionalString(rs, DISPLAY_RANGE_MIN_COL_NAME, null),
        getRsOptionalString(rs, DISPLAY_RANGE_MAX_COL_NAME, null),
        getRsRequiredString(rs, RANGE_MIN_COL_NAME),
        getRsRequiredString(rs, RANGE_MAX_COL_NAME),
        1,
        includeBinInfo ? getRsRequiredString(rs, BIN_WIDTH_COMPUTED_COL_NAME) : null,
        includeBinInfo ? getRsOptionalString(rs, BIN_WIDTH_OVERRIDE_COL_NAME, null) : null
    );
  }

  public static NumberDistributionConfig<Double> createFloatDistributionConfig(
      ResultSet rs, boolean includeBinInfo) throws SQLException {
    return new NumberDistributionConfig<>(
        getDoubleFromString(rs, DISPLAY_RANGE_MIN_COL_NAME, false),
        getDoubleFromString(rs, DISPLAY_RANGE_MAX_COL_NAME, false),
        getDoubleFromString(rs, RANGE_MIN_COL_NAME, true),
        getDoubleFromString(rs, RANGE_MAX_COL_NAME, true),
        includeBinInfo ? getDoubleFromString(rs, BIN_WIDTH_COMPUTED_COL_NAME, true) : null,
        includeBinInfo ? getDoubleFromString(rs, BIN_WIDTH_OVERRIDE_COL_NAME, false) : null
    );
  }

  public static NumberDistributionConfig<Long> createIntegerDistributionConfig(
      ResultSet rs, boolean includeBinInfo) throws SQLException {
    return new NumberDistributionConfig<>(
        getIntegerFromString(rs, DISPLAY_RANGE_MIN_COL_NAME, false),
        getIntegerFromString(rs, DISPLAY_RANGE_MAX_COL_NAME, false),
        getIntegerFromString(rs, RANGE_MIN_COL_NAME, true),
        getIntegerFromString(rs, RANGE_MAX_COL_NAME, true),
        includeBinInfo ? getIntegerFromString(rs, BIN_WIDTH_COMPUTED_COL_NAME, true) : null,
        includeBinInfo ? getIntegerFromString(rs, BIN_WIDTH_OVERRIDE_COL_NAME, false) : null
    );
  }
}
