package org.veupathdb.service.eda.ss.model.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.veupathdb.service.eda.ss.Resources;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;
import org.veupathdb.service.eda.ss.model.variable.FloatingPointVariable;
import org.veupathdb.service.eda.ss.model.variable.IntegerVariable;
import org.veupathdb.service.eda.ss.model.variable.LongitudeVariable;
import org.veupathdb.service.eda.ss.model.variable.StringVariable;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableDataShape;
import org.veupathdb.service.eda.ss.model.variable.VariableDisplayType;
import org.veupathdb.service.eda.ss.model.variable.VariableType;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;
import org.veupathdb.service.eda.ss.model.variable.VariablesCategory;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.gusdb.fgputil.functional.Functions.doThrow;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeGraph.Columns.*;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getDoubleFromString;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getIntegerFromString;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getRsOptionalLong;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getRsRequiredBoolean;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getRsRequiredString;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getRsOptionalString;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.parseJsonArrayOfString;

class VariableFactory {

  private final DataSource _dataSource;

  public VariableFactory(DataSource dataSource) {
    _dataSource = dataSource;
  }

  List<Variable> loadVariables(Entity entity) {

    String sql = generateStudyVariablesListSql(entity);
    
    return new SQLRunner(_dataSource, sql, "Get entity variables metadata for: '" + entity.getDisplayName() + "'").executeQuery(rs -> {
      List<Variable> variables = new ArrayList<>();
      while (rs.next()) {
        variables.add(createVariableFromResultSet(rs, entity));
      }
      return variables;
    });
  }

  static String generateStudyVariablesListSql(Entity entity) {
    // This SQL safe from injection because entities declare their own table names (no parameters)
    // TODO: remove hack distinct
    return "SELECT distinct " + String.join(", ", DB.Tables.AttributeGraph.Columns.ALL) + NL
        + "FROM " + Resources.getAppDbSchema() + DB.Tables.AttributeGraph.NAME(entity) + NL
        + "ORDER BY " + DB.Tables.AttributeGraph.Columns.VARIABLE_ID_COL_NAME;  // stable ordering supports unit testing
  }

  static Variable createVariableFromResultSet(ResultSet rs, Entity entity) throws SQLException {

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
        ? createValueVarFromResultSet(rs, varProps)
        : new VariablesCategory(varProps);
  }

  static Variable createValueVarFromResultSet(ResultSet rs, Variable.Properties varProps) {
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
          rs.getBoolean(IMPUTE_ZERO)
      );

      return switch(valueProps.type) {

        case NUMBER ->
            new FloatingPointVariable(varProps, valueProps, createFloatDistributionConfig(rs, true), createFloatProperties(rs));

        case LONGITUDE ->
            new LongitudeVariable(varProps, valueProps, new LongitudeVariable.Properties(
                getRsOptionalLong(rs, PRECISION_COL_NAME, 1L)
            ));

        case INTEGER ->
            new IntegerVariable(varProps, valueProps, createIntegerDistributionConfig(rs, true), createIntegerProperties(rs));

        case DATE ->
            new DateVariable(varProps, valueProps, createDateDistributionConfig(valueProps.dataShape, rs, true));

        case STRING ->
            new StringVariable(varProps, valueProps);

        default -> doThrow(() ->
            new RuntimeException("Entity:  " + varProps.entity.getId() +
              " variable: " + varProps.id + " has unrecognized type " + valueProps.type));
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

  public static FloatingPointVariable.Properties createFloatProperties(ResultSet rs) throws SQLException {
    return new FloatingPointVariable.Properties(
        getRsOptionalString(rs, UNITS_COL_NAME, ""),
        getRsOptionalLong(rs, PRECISION_COL_NAME, 1L)
    );
  }

  public static DateDistributionConfig createDateDistributionConfig(
      VariableDataShape dataShape, ResultSet rs, boolean includeBinInfo) throws SQLException {
    return new DateDistributionConfig(!includeBinInfo,
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
