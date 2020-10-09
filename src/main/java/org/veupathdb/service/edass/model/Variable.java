package org.veupathdb.service.edass.model;

public class Variable {
  private String name;
  private String id;
  private Entity entity;
  private VariableType type;
  private Resolution resolution;
  
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
    return entity.getEntityId();
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
