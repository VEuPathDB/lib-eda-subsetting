package org.veupathdb.service.edass.model;

import org.gusdb.fgputil.db.runner.SQLRunner;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.edass.model.RdbmsColumnNames.*;

class VariableResultSetUtils {

  static List<Variable> getEntityVariables(DataSource datasource, Entity entity) {
    
    String sql = generateStudyVariablesListSql(entity.getVariablesTableName());
    
    return new SQLRunner(datasource, sql).executeQuery(rs -> {
      List<Variable> variables = new ArrayList<>();
      while (rs.next()) {
        variables.add(createVariableFromResultSet(rs, entity));
      }
      return variables;
    });
  }

  static String generateStudyVariablesListSql(String variablesTableName) {
    String[] selectCols = {VARIABLE_ID_COL_NAME, VARIABLE_TYPE_COL_NAME,
            DATA_SHAPE_COL_NAME, DISPLAY_TYPE_COL_NAME, HAS_VALUES_COL_NAME, UNITS_COL_NAME, MULTIVALUED_COL_NAME, PRECISION_COL_NAME, PROVIDER_LABEL_COL_NAME, DISPLAY_NAME_COL_NAME, VARIABLE_PARENT_ID_COL_NAME};
    
    return "SELECT " + String.join(", ", selectCols) + NL
        + "FROM " + variablesTableName + NL
        + "ORDER BY " + VARIABLE_ID_COL_NAME;  // stable ordering supports unit testing
  }

  //   public Variable(String providerLabel, String id, Entity entity, VariableType type, VariableDataShape dataShape,
  //                  VariableDisplayType displayType, boolean hasValues, String units, Integer precision, String displayName, String parentId) {
  static Variable createVariableFromResultSet(ResultSet rs, Entity entity) {
    try {
      return new Variable(
          getRsStringNotNull(rs, PROVIDER_LABEL_COL_NAME),
          getRsStringNotNull(rs, VARIABLE_ID_COL_NAME),
          entity,
          Variable.VariableType.fromString(getRsStringNotNull(rs, VARIABLE_TYPE_COL_NAME)),
          Variable.VariableDataShape.fromString(getRsStringNotNull(rs, DATA_SHAPE_COL_NAME)),
          Variable.VariableDisplayType.fromString(getRsStringNotNull(rs, DISPLAY_TYPE_COL_NAME)),
          rs.getBoolean(HAS_VALUES_COL_NAME),
          rs.getString(UNITS_COL_NAME),
          rs.getInt(PRECISION_COL_NAME),
          getRsStringNotNull(rs, DISPLAY_NAME_COL_NAME),
          rs.getString(VARIABLE_PARENT_ID_COL_NAME)
          );
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
}
