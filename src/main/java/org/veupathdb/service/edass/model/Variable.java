package org.veupathdb.service.edass.model;

import java.sql.ResultSet;
import java.util.Date;
import java.util.Map;

import org.gusdb.fgputil.FormatUtil;

import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;

public class Variable {
  private String providerLabel;
  private String id;
  private Entity entity;
  private VariableType type;
  private IsContinuous isContinuous;
  private String units;
  private Integer precision;
  private String displayName;
  private String parentId;
  
  public static enum VariableType {
    STRING ("string_value", rs -> rs.getString("string_value"), "string"),  
    NUMBER ("number_value", rs -> String.valueOf(rs.getDouble("number_value")), "number"),
    DATE   ("date_value", rs -> FormatUtil.formatDate(new Date(rs.getTimestamp("date_value").getTime())), "date");

    private final String tallTableColumnName;
    private final String typeString;
    private final FunctionWithException<ResultSet, String> resultSetToStringValue;
    Map<String, VariableType> typeStringMap;

    VariableType(String tallTableColumnName, FunctionWithException<ResultSet, String> resultSetToStringValue, String typeString) {
      this.tallTableColumnName = tallTableColumnName;
      this.resultSetToStringValue = resultSetToStringValue;
      this.typeString = typeString;
    }
    
    public String getTallTableColumnName() {
      return this.tallTableColumnName;
    }   
    
    public String getTypeString() {
      return this.typeString;
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
  
  public enum IsContinuous {
    FALSE,
    TRUE;
    
    public static IsContinuous fromBoolean(Boolean bool) {
      return bool? TRUE : FALSE;
    }
  }
  
  public Variable(String providerLabel, String id, Entity entity, VariableType type, IsContinuous isContinuous) {

    this.providerLabel = providerLabel;
    this.id = id;
    this.entity = entity;
    this.type = type;
    this.isContinuous = isContinuous;
  }

  public Variable(String providerLabel, String id, Entity entity, VariableType type, IsContinuous isContinuous,
      String units, Integer precision, String displayName, String parentId) {
    this.providerLabel = providerLabel;
    this.id = id;
    this.entity = entity;
    this.type = type;
    this.isContinuous = isContinuous;
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
  
  public IsContinuous getIsContinuous() {
    return isContinuous;
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
