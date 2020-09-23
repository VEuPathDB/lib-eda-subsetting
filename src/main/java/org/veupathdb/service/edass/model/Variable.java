package org.veupathdb.service.edass.model;

public class Variable {
  private String name;
  private String id;
  private String entityId;
  
  public Variable(String name, String id, String entityId) {

    this.name = name;
    this.id = id;
    this.entityId = entityId;
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
}
