package org.veupathdb.service.eda.subset.model.db;

import org.gusdb.fgputil.db.runner.SQLRunner;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.subset.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.subset.model.reducer.BinaryMetadataProvider;
import org.veupathdb.service.eda.subset.model.variable.BinaryProperties;
import org.veupathdb.service.eda.subset.model.variable.DateVariable;
import org.veupathdb.service.eda.subset.model.variable.FloatingPointVariable;
import org.veupathdb.service.eda.subset.model.variable.IntegerVariable;
import org.veupathdb.service.eda.subset.model.variable.LongitudeVariable;
import org.veupathdb.service.eda.subset.model.variable.StringVariable;
import org.veupathdb.service.eda.subset.model.variable.Variable;
import org.veupathdb.service.eda.subset.model.variable.VariableDataShape;
import org.veupathdb.service.eda.subset.model.variable.VariableDisplayType;
import org.veupathdb.service.eda.subset.model.variable.VariableScale;
import org.veupathdb.service.eda.subset.model.variable.VariableType;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;
import org.veupathdb.service.eda.subset.model.variable.VariablesCategory;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.gusdb.fgputil.FormatUtil.NL;

public class VariableFactory {

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
        ResultSetUtils.getRsOptionalString(rs, DB.Tables.AttributeGraph.Columns.PROVIDER_LABEL_COL_NAME, "No Provider Label available"), // TODO remove hack when in db
        ResultSetUtils.getRsRequiredString(rs, DB.Tables.AttributeGraph.Columns.VARIABLE_ID_COL_NAME),
        entity,
        VariableDisplayType.fromString(ResultSetUtils.getRsOptionalString(rs, DB.Tables.AttributeGraph.Columns.DISPLAY_TYPE_COL_NAME, VariableDisplayType.DEFAULT.getType())),
        ResultSetUtils.getRsRequiredString(rs, DB.Tables.AttributeGraph.Columns.DISPLAY_NAME_COL_NAME),
        ResultSetUtils.getRsOptionalLong(rs, DB.Tables.AttributeGraph.Columns.DISPLAY_ORDER_COL_NAME, null),
        ResultSetUtils.getRsOptionalString(rs, DB.Tables.AttributeGraph.Columns.VARIABLE_PARENT_ID_COL_NAME, null),
        ResultSetUtils.getRsOptionalString(rs, DB.Tables.AttributeGraph.Columns.DEFINITION_COL_NAME, ""),
        ResultSetUtils.parseJsonArrayOfString(rs, DB.Tables.AttributeGraph.Columns.HIDE_FROM_COL_NAME));
    // Only set binary properties if binaryMetadataProvider is present.
    Optional<BinaryProperties> binaryProperties = binaryMetadataProvider.flatMap(provider ->
        provider.getBinaryProperties(entity.getStudyAbbrev(), entity, varProps.id));
    return ResultSetUtils.getRsRequiredBoolean(rs, DB.Tables.AttributeGraph.Columns.HAS_VALUES_COL_NAME)
        ? createValueVarFromResultSet(rs, varProps, binaryProperties.orElse(null))
        : new VariablesCategory(varProps);
  }

  static Variable createValueVarFromResultSet(ResultSet rs,
                                              Variable.Properties varProps,
                                              BinaryProperties binaryProperties) {
    try {
      VariableWithValues.Properties valueProps = new VariableWithValues.Properties(
          VariableType.fromString(ResultSetUtils.getRsRequiredString(rs, DB.Tables.AttributeGraph.Columns.VARIABLE_TYPE_COL_NAME)),
          VariableDataShape.fromString(ResultSetUtils.getRsRequiredString(rs, DB.Tables.AttributeGraph.Columns.DATA_SHAPE_COL_NAME)),
          ResultSetUtils.parseJsonArrayOfString(rs, DB.Tables.AttributeGraph.Columns.VOCABULARY_COL_NAME),
          rs.getLong(DB.Tables.AttributeGraph.Columns.DISTINCT_VALUES_COUNT_COL_NAME),
          rs.getBoolean(DB.Tables.AttributeGraph.Columns.IS_TEMPORAL_COL_NAME),
          rs.getBoolean(DB.Tables.AttributeGraph.Columns.IS_FEATURED_COL_NAME),
          rs.getBoolean(DB.Tables.AttributeGraph.Columns.IS_MERGE_KEY_COL_NAME),
          rs.getBoolean(DB.Tables.AttributeGraph.Columns.IS_MULTI_VALUED_COL_NAME),
          rs.getBoolean(DB.Tables.AttributeGraph.Columns.IMPUTE_ZERO),
          rs.getBoolean(DB.Tables.AttributeGraph.Columns.HAS_STUDY_DEPENDENT_VOCABULARY),
          rs.getString(DB.Tables.AttributeGraph.Columns.VARIABLE_SPEC_TO_IMPUTE_ZEROES_FOR));

      switch(valueProps.type) {

        case NUMBER: return
            new FloatingPointVariable(varProps, valueProps, createFloatDistributionConfig(rs, true), createFloatProperties(rs, true));

        case LONGITUDE: return
            new LongitudeVariable(varProps, valueProps, new LongitudeVariable.Properties(
                ResultSetUtils.getRsOptionalLong(rs, DB.Tables.AttributeGraph.Columns.PRECISION_COL_NAME, 1L)
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
        ResultSetUtils.getRsOptionalString(rs, DB.Tables.AttributeGraph.Columns.UNITS_COL_NAME, "")
    );
  }

  public static FloatingPointVariable.Properties createFloatProperties(ResultSet rs, boolean sqlContainsScale) throws SQLException {
    return new FloatingPointVariable.Properties(
        ResultSetUtils.getRsOptionalString(rs, DB.Tables.AttributeGraph.Columns.UNITS_COL_NAME, ""),
        ResultSetUtils.getRsOptionalLong(rs, DB.Tables.AttributeGraph.Columns.PRECISION_COL_NAME, 1L),
        VariableScale.findByValue(sqlContainsScale ? ResultSetUtils.getRsOptionalString(rs, DB.Tables.AttributeGraph.Columns.SCALE_COL_NAME, null) : null)
    );
  }

  public static DateDistributionConfig createDateDistributionConfig(
      VariableDataShape dataShape, ResultSet rs, boolean includeBinInfo) throws SQLException {
    return new DateDistributionConfig(includeBinInfo,
        dataShape,
        ResultSetUtils.getRsOptionalString(rs, DB.Tables.AttributeGraph.Columns.DISPLAY_RANGE_MIN_COL_NAME, null),
        ResultSetUtils.getRsOptionalString(rs, DB.Tables.AttributeGraph.Columns.DISPLAY_RANGE_MAX_COL_NAME, null),
        ResultSetUtils.getRsRequiredString(rs, DB.Tables.AttributeGraph.Columns.RANGE_MIN_COL_NAME),
        ResultSetUtils.getRsRequiredString(rs, DB.Tables.AttributeGraph.Columns.RANGE_MAX_COL_NAME),
        1,
        includeBinInfo ? ResultSetUtils.getRsRequiredString(rs, DB.Tables.AttributeGraph.Columns.BIN_WIDTH_COMPUTED_COL_NAME) : null,
        includeBinInfo ? ResultSetUtils.getRsOptionalString(rs, DB.Tables.AttributeGraph.Columns.BIN_WIDTH_OVERRIDE_COL_NAME, null) : null
    );
  }

  public static NumberDistributionConfig<Double> createFloatDistributionConfig(
      ResultSet rs, boolean includeBinInfo) throws SQLException {
    return new NumberDistributionConfig<>(
        ResultSetUtils.getDoubleFromString(rs, DB.Tables.AttributeGraph.Columns.DISPLAY_RANGE_MIN_COL_NAME, false),
        ResultSetUtils.getDoubleFromString(rs, DB.Tables.AttributeGraph.Columns.DISPLAY_RANGE_MAX_COL_NAME, false),
        ResultSetUtils.getDoubleFromString(rs, DB.Tables.AttributeGraph.Columns.RANGE_MIN_COL_NAME, true),
        ResultSetUtils.getDoubleFromString(rs, DB.Tables.AttributeGraph.Columns.RANGE_MAX_COL_NAME, true),
        includeBinInfo ? ResultSetUtils.getDoubleFromString(rs, DB.Tables.AttributeGraph.Columns.BIN_WIDTH_COMPUTED_COL_NAME, true) : null,
        includeBinInfo ? ResultSetUtils.getDoubleFromString(rs, DB.Tables.AttributeGraph.Columns.BIN_WIDTH_OVERRIDE_COL_NAME, false) : null
    );
  }

  public static NumberDistributionConfig<Long> createIntegerDistributionConfig(
      ResultSet rs, boolean includeBinInfo) throws SQLException {
    return new NumberDistributionConfig<>(
        ResultSetUtils.getIntegerFromString(rs, DB.Tables.AttributeGraph.Columns.DISPLAY_RANGE_MIN_COL_NAME, false),
        ResultSetUtils.getIntegerFromString(rs, DB.Tables.AttributeGraph.Columns.DISPLAY_RANGE_MAX_COL_NAME, false),
        ResultSetUtils.getIntegerFromString(rs, DB.Tables.AttributeGraph.Columns.RANGE_MIN_COL_NAME, true),
        ResultSetUtils.getIntegerFromString(rs, DB.Tables.AttributeGraph.Columns.RANGE_MAX_COL_NAME, true),
        includeBinInfo ? ResultSetUtils.getIntegerFromString(rs, DB.Tables.AttributeGraph.Columns.BIN_WIDTH_COMPUTED_COL_NAME, true) : null,
        includeBinInfo ? ResultSetUtils.getIntegerFromString(rs, DB.Tables.AttributeGraph.Columns.BIN_WIDTH_OVERRIDE_COL_NAME, false) : null
    );
  }
}
