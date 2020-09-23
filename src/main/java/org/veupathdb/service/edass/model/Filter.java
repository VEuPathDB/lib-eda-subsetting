package org.veupathdb.service.edass.model;

public abstract class Filter {
  protected String entityPrimaryKeyColumunName;
  protected String entityTableName;
  protected String entityId;

  public Filter(String entityId, String entityPrimaryKeyColumunName, String entityTableName) {
    this.entityPrimaryKeyColumunName = entityPrimaryKeyColumunName;
    this.entityTableName = entityTableName;
  }

  public abstract String getSql();

  public String getEntityPrimaryKeyColumunName() {
    return entityPrimaryKeyColumunName;
  }

  public String getEntityTableName() {
    return entityTableName;
  }

  public String getEntityId() {
    return entityId;
  }
  
  
}
