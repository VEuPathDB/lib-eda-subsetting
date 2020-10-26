package org.veupathdb.service.edass.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;
import javax.ws.rs.InternalServerErrorException;

import org.gusdb.fgputil.db.runner.SQLRunner;

import static org.veupathdb.service.edass.model.RdbmsColumnNames.*;

class VariableResultSetUtils {

  static List<Variable> getStudyVariables(DataSource datasource, String studyId, Map<String, Entity> entityIdMap) {
    
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
    String[] selectCols = {NAME_COL_NAME, VARIABLE_ID_COL_NAME, ENTITY_ID_COL_NAME, VARIABLE_TYPE_COL_NAME, 
        CONTINUOUS_COL_NAME, UNITS_COL_NAME, PRECISION_COL_NAME, DISPLAY_NAME_COL_NAME, VARIABLE_PARENT_ID_COL_NAME};
    
    return "SELECT " + String.join(", ", selectCols) + nl
        + "FROM " + ENTITY_TABLE_NAME + " e, " + nl
        + "  " + VARIABLE_TABLE_NAME + " v," + nl
        + "  " + VARIABLE_TYPE_TABLE_NAME + " t" + nl
        + "WHERE e." + ENTITY_ID_COL_NAME + " = v." + ENTITY_ID_COL_NAME + nl
        + "WHERE v." + VARIABLE_ID_COL_NAME + " = n." + VARIABLE_ID_COL_NAME + nl
        + "AND " + STUDY_ID_COL_NAME + " = " + studyId;
  }

  private static Variable createVariableFromResultSet(ResultSet rs, Map<String, Entity> entityIdMap) {
    try {
      String entityId = getRsStringNotNull(rs, ENTITY_ID_COL_NAME);
      if (!entityIdMap.containsKey(entityId))
        throw new InternalServerErrorException("Variable's entity ID not found for this study: " + entityId);
      Entity entity = entityIdMap.get(entityId);
      return new Variable(
          getRsStringNotNull(rs, NAME_COL_NAME),
          getRsStringNotNull(rs, VARIABLE_ID_COL_NAME),
          entity,
          Variable.VariableType.valueOf(getRsStringNotNull(rs, VARIABLE_TYPE_COL_NAME)),
          Variable.IsContinuous.fromBoolean(rs.getBoolean(PRECISION_COL_NAME)),
          getRsStringNotNull(rs, UNITS_COL_NAME),
          rs.getInt(PRECISION_COL_NAME),
          getRsStringNotNull(rs, DISPLAY_NAME_COL_NAME),
          getRsStringNotNull(rs, VARIABLE_PARENT_ID_COL_NAME)
          );
    }
    catch (SQLException e) {
      throw new InternalServerErrorException(e);
    }
  }
  
}
