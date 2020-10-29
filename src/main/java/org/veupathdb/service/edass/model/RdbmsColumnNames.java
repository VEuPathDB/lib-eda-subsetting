package org.veupathdb.service.edass.model;

import java.sql.ResultSet;
import java.sql.SQLException;

class RdbmsColumnNames {

  static final String VARIABLE_VALUE_COL_NAME = "value";
  
  // common
  static final String DESCRIP_COL_NAME = "description";
  static final String DISPLAY_NAME_COL_NAME = "display_name";
  static final String DISPLAY_NAME_PLURAL_COL_NAME = "display_name_plural";
  static final String STUDY_ID_COL_NAME = "study_id";
  static final String ENTITY_ID_COL_NAME = "entity_id";

  // Entity table and EntityName table
  static final String ENTITY_TABLE_NAME = "entity";
  static final String ENTITY_NAME_TABLE_NAME = "entityName";
  static final String ENTITY_NAME_ID_COL_NAME = "entity_name_id";
  static final String ENTITY_PARENT_ID_COL_NAME = "parent_entity_id";
  
  // Variable table and VariableType table
  static final String VARIABLE_TABLE_NAME = "variable";
  static final String VARIABLE_TYPE_TABLE_NAME = "variableType";
  static final String VARIABLE_ID_COL_NAME = "variable_id";
  static final String VARIABLE_TYPE_COL_NAME = "variable_type";
  static final String VARIABLE_TYPE_ID_COL_NAME = "variable_type_id";
  static final String PROVIDER_LABEL_COL_NAME = "provider_label";
  static final String CONTINUOUS_COL_NAME = "is_continuous";
  static final String UNITS_COL_NAME = "units";
  static final String PRECISION_COL_NAME = "precision";
  static final String VARIABLE_PARENT_ID_COL_NAME = "parent_variable_id";
  
  // Tall table
  static final String STRING_VALUE_COL_NAME = "string_value";
  static final String DATE_VALUE_COL_NAME = "date_value";
  static final String NUMBER_VALUE_COL_NAME = "number_value";
  
  static String getRsStringNotNull(ResultSet rs, String colName) throws SQLException {
    if (rs.getString(colName) == null) 
      throw new RuntimeException("Found a null for variable column: " + colName);
    return rs.getString(colName);
  }
}
