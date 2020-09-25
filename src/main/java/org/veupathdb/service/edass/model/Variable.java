package org.veupathdb.service.edass.model;

public class Variable {
  private String name;
  private String id;
  private String entityId;
  private VariableType type;
  
  public enum VariableType {
    STRING ("string_value"),  
    NUMBER ("number_value"),
    DATE   ("date_value");

    private final String tallTableColumnName;

    VariableType(String tallTableColumnName) {
      this.tallTableColumnName = tallTableColumnName;
    }
    
    public String getTallTableColumnName() {
      return this.tallTableColumnName;
    }    
  }
  
  public Variable(String name, String id, String entityId, VariableType type) {

    this.name = name;
    this.id = id;
    this.entityId = entityId;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public String getId() {
    return id;
  }

  public String getEntityId() {
    return entityId;
  } 
  
  public VariableType getVariableType() {
    return type;
  }
}
