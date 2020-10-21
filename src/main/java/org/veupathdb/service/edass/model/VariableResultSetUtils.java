package org.veupathdb.service.edass.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;
import javax.ws.rs.InternalServerErrorException;

import org.gusdb.fgputil.db.runner.SQLRunner;

public class VariableResultSetUtils {

  static final String NAME_COL_NAME = "name";
  static final String ID_COL_NAME = "variable_id";
  static final String ENTITY_ID_COL_NAME = "entity_id";
  static final String DATA_TYPE_COL_NAME = "data_type";
  static final String RESOLUTION_COL_NAME = "resolution";
  static final String UNITS_COL_NAME = "units";
  static final String PRECISION_COL_NAME = "precision";
  static final String DISPLAY_NAME_COL_NAME = "display_name";
  static final String PARENT_ID_COL_NAME = "parent_id";

  public static List<Variable> getStudyVariables(DataSource datasource, String studyId, Map<String, Entity> entityIdMap) {
    
    String sql = generateStudyVariablesListSql(studyId);
    
    return new SQLRunner(datasource, sql).executeQuery(rs -> {
      List<Variable> variables = new ArrayList<Variable>();
      while (rs.next()) {
        variables.add(createVariableFromResultSet(rs, entityIdMap));
      }
      return variables;
    });
  }
  
  private static String generateStudyVariablesListSql(String studyId) {
    // TODO
    return null;
  }

  private static Variable createVariableFromResultSet(ResultSet rs, Map<String, Entity> entityIdMap) {
    try {
      String entityId = getRsStringNotNull(rs, ENTITY_ID_COL_NAME);
      if (!entityIdMap.containsKey(entityId))
        throw new InternalServerErrorException("Variable's entity ID not found for this study: " + entityId);
      Entity entity = entityIdMap.get(entityId);
      return new Variable(
          getRsStringNotNull(rs, NAME_COL_NAME),
          getRsStringNotNull(rs, ID_COL_NAME),
          entity,
          Variable.VariableType.valueOf(getRsStringNotNull(rs, DATA_TYPE_COL_NAME)),
          Variable.Resolution.valueOf(getRsStringNotNull(rs, RESOLUTION_COL_NAME)),
          getRsStringNotNull(rs, UNITS_COL_NAME),
          rs.getInt(PRECISION_COL_NAME),
          getRsStringNotNull(rs, DISPLAY_NAME_COL_NAME),
          getRsStringNotNull(rs, PARENT_ID_COL_NAME)
          );
    }
    catch (SQLException e) {
      throw new InternalServerErrorException(e);
    }
  }
  
  static String getRsStringNotNull(ResultSet rs, String colName) throws SQLException {
    if (rs.getString(colName) == null) 
      throw new InternalServerErrorException("Found a null for variable column: " + colName);
    return rs.getString(colName);
  }

}
