package org.veupathdb.service.edass.model;

import java.sql.ResultSet;
import java.util.Date;
import org.gusdb.fgputil.FormatUtil;

import javax.ws.rs.InternalServerErrorException;

import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;

public class Variable {
  private String name;
  private String id;
  private Entity entity;
  private VariableType type;
  private Resolution resolution;
  
  public static enum VariableType {
    STRING ("string_value", rs -> rs.getString("string_value")),  
    NUMBER ("number_value", rs -> String.valueOf(rs.getDouble("number_value"))),
    DATE   ("date_value", rs -> FormatUtil.formatDate(new Date(rs.getTimestamp("date_value").getTime())));

    private final String tallTableColumnName;
    private final FunctionWithException<ResultSet, String> resultSetToStringValue;

    VariableType(String tallTableColumnName, FunctionWithException<ResultSet, String> resultSetToStringValue) {
      this.tallTableColumnName = tallTableColumnName;
      this.resultSetToStringValue = resultSetToStringValue;
    }
    
    public String getTallTableColumnName() {
      return this.tallTableColumnName;
    }   
    
    public String convertRowValueToStringValue(ResultSet rs) {
      try {
        return resultSetToStringValue.apply(rs);
      }
      catch (Exception e) {
        throw new InternalServerErrorException(e);
      }
    }
  }
  
  public enum Resolution {
    CONTINUOUS,
    CATEGORICAL;
  }
  
  public Variable(String name, String id, Entity entity, VariableType type, Resolution resolution) {

    this.name = name;
    this.id = id;
    this.entity = entity;
    this.type = type;
    this.resolution = resolution;
  }

  public String getName() {
    return name;
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
  
  public VariableType getVariableType() {
    return type;
  }

  public Resolution getResolution() {
    return resolution;
  }
  
}
