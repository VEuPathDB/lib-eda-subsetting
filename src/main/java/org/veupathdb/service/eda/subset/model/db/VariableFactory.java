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
import org.veupathdb.service.eda.subset.model.variable.Utf8EncodingLengthProperties;
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
import static org.veupathdb.service.eda.subset.model.db.DB.Tables.AttributeGraph.Columns.*;
import static org.veupathdb.service.eda.subset.model.db.ResultSetUtils.*;

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

    // metadataProvider will be empty IFF
    //   1. binaryMetadataProvider passed to the constructor above is null, or
    //   2. _shouldAppendMetaForStudy returns false when called on the next line
    boolean appendMetaForStudy = _shouldAppendMetaForStudy.apply(studyAbbrev);
    Optional<BinaryMetadataProvider> metadataProvider = appendMetaForStudy
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
    return "SELECT " + String.join(", ", DB.Tables.AttributeGraph.Columns.ALL) + NL
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
    return getRsRequiredBoolean(rs, HAS_VALUES_COL_NAME)
      ? createValueVarFromResultSet(rs, varProps, binaryMetadataProvider
        .flatMap(provider -> provider.getBinaryProperties(entity.getStudyAbbrev(), entity, varProps.id)).orElse(null))
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

      return switch (valueProps.type) {
        case NUMBER -> new FloatingPointVariable(varProps, valueProps, createFloatDistributionConfig(rs, true), createFloatProperties(rs, true), (Utf8EncodingLengthProperties) binaryProperties);
        case LONGITUDE -> new LongitudeVariable(varProps, valueProps, new LongitudeVariable.Properties(getRsOptionalLong(rs, PRECISION_COL_NAME, 1L)), (Utf8EncodingLengthProperties) binaryProperties);
        case INTEGER -> new IntegerVariable(varProps, valueProps, createIntegerDistributionConfig(rs, true), createIntegerProperties(rs));
        case DATE -> new DateVariable(varProps, valueProps, createDateDistributionConfig(valueProps.dataShape, rs, true));
        case STRING -> new StringVariable(varProps, valueProps, (Utf8EncodingLengthProperties) binaryProperties);
      };
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
