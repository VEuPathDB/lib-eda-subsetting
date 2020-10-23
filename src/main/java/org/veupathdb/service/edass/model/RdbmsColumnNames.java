package org.veupathdb.service.edass.model;

public class RdbmsColumnNames {
  
  public static final String nl = System.lineSeparator();

  public static final String VARIABLE_VALUE_COL_NAME = "value";
  
  // common
  public static final String NAME_COL_NAME = "name";
  public static final String DESCRIP_COL_NAME = "description";
  public static final String DISPLAY_NAME_COL_NAME = "display_name";
  public static final String STUDY_ID_COL_NAME = "study_id";
  public static final String ENTITY_ID_COL_NAME = "entity_id";

  // Entity table and EntityName table
  public static final String ENTITY_TABLE_NAME = "entity";
  public static final String ENTITY_NAME_TABLE_NAME = "entityName";
  public static final String ENTITY_NAME_ID_COL_NAME = "entity_name_id";
  public static final String ENTITY_PARENT_ID_COL_NAME = "parent_entity_id";
  
  // Variable table and VariableType table
  public static final String VARIABLE_TABLE_NAME = "variable";
  public static final String VARIABLE_TYPE_TABLE_NAME = "variableType";
  public static final String VARIABLE_ID_COL_NAME = "variable_id";
  public static final String VARIABLE_TYPE_COL_NAME = "variable_type";
  public static final String CONTINUOUS_COL_NAME = "isContinuous";
  public static final String UNITS_COL_NAME = "units";
  public static final String PRECISION_COL_NAME = "precision";
  public static final String VARIABLE_PARENT_ID_COL_NAME = "parent_variable_id";
  
  // Tall table
  public static final String STRING_VALUE_COL_NAME = "string_value";
  public static final String DATE_VALUE_COL_NAME = "date_value";
  public static final String NUMBER_VALUE_COL_NAME = "number_value";

}
