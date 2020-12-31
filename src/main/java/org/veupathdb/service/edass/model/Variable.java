package org.veupathdb.service.edass.model;

import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;

import java.sql.ResultSet;
import java.util.Date;

public class Variable {
  private final String providerLabel;
  private final String id;
  private final Entity entity;
  private final VariableType type;
  private final VariableDataShape dataShape;
  private final VariableDisplayType displayType;
  private final boolean hasValues;
  private final String units;
  private final Integer precision;
  private final String displayName;
  private final String parentId;

  public boolean getHasValues() {
    return hasValues;
  }

  public VariableDisplayType getDisplayType() {
    return displayType;
  }

  public enum VariableType {
    STRING ("string_value", rs -> rs.getString("string_value"), "string"),  
    NUMBER ("number_value", rs -> String.valueOf(rs.getDouble("number_value")), "number"),
    DATE   ("date_value", rs -> FormatUtil.formatDate(new Date(rs.getTimestamp("date_value").getTime())), "date");

    private final String tallTableColumnName;
    private final String typeString;
    private final FunctionWithException<ResultSet, String> resultSetToStringValue;

    VariableType(String tallTableColumnName, FunctionWithException<ResultSet, String> resultSetToStringValue, String typeString) {
      this.tallTableColumnName = tallTableColumnName;
      this.resultSetToStringValue = resultSetToStringValue;
      this.typeString = typeString;
    }
    
    public String getTallTableColumnName() {
      return this.tallTableColumnName;
    }

    public static VariableType fromTypeString(String str) {
      if (str.equals(STRING.typeString)) return STRING;
      else if (str.equals(NUMBER.typeString)) return NUMBER;
      else if (str.equals(DATE.typeString)) return DATE;
      else throw new RuntimeException("Illegal variable type string: " + str);
    }
    
    public String convertRowValueToStringValue(ResultSet rs) {
      try {
        return resultSetToStringValue.apply(rs);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  public enum VariableDataShape {
    CONTINUOUS,
    CATEGORICAL,
    ORDINAL,
    BINARY;
    
    public static VariableDataShape fromString(String shape) {

      VariableDataShape v;

      switch (shape) {
        case "continuous" -> v = CONTINUOUS;
        case "categorical" -> v = CATEGORICAL;
        case "ordinal" -> v = ORDINAL;
        case "binary" -> v = BINARY;
        default -> throw new RuntimeException("Unrecognized data shape: " + shape);
      }
      return v;
    }
  }

  public enum VariableDisplayType {
    DEFAULT,
    MULTIFILTER;

    public static VariableDisplayType fromString(String displayType) {

      VariableDisplayType t;

      switch (displayType) {
        case "default" -> t = DEFAULT;
        case "multifilter" -> t = MULTIFILTER;
        default -> throw new RuntimeException("Unrecognized variable display type: " + displayType);
      };
      return t;
    }
  }

  public Variable(String providerLabel, String id, Entity entity, VariableType type, VariableDataShape dataShape,
                  VariableDisplayType displayType, boolean hasValues, String units, Integer precision, String displayName, String parentId) {
    this.providerLabel = providerLabel;
    this.id = id;
    this.entity = entity;
    this.type = type;
    this.dataShape = dataShape;
    this.displayType = displayType;
    this.hasValues = hasValues;
    this.units = units;
    this.precision = precision;
    this.displayName = displayName;
    this.parentId = parentId;

  }

  public String getProviderLabel() {
    return providerLabel;
  }

  public String getId() {
    return id;
  }

  public String getEntityId() {
    return entity.getId();
  } 
  
  public Entity getEntity() {
    return entity;
  } 
  
  public VariableDataShape getDataShape() {
    return dataShape;
  }
  
  public String getUnits() {
    return units;
  }

  public Integer getPrecision() {
    return precision;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getParentId() {
    return parentId;
  }

  public VariableType getType() {
    return type;
  }
}
