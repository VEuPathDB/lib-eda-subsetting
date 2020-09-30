package org.veupathdb.service.edass.model;

public abstract class Filter {
  protected String entityPrimaryKeyColumunName;
  protected String entityTableName;
  protected String entityId;
  protected String variableName;
  
  protected static final String nl = System.lineSeparator();

  public Filter(String entityId, String entityPrimaryKeyColumunName, String entityTableName, String variableName) {
    this.entityId = entityId;
    this.entityPrimaryKeyColumunName = entityPrimaryKeyColumunName;
    this.entityTableName = entityTableName;
    this.variableName = variableName;
  }

  public String getSql() {
    return "SELECT " + entityPrimaryKeyColumunName + " FROM " + entityTableName + nl
        + "WHERE ontology_term_name = '" + variableName + "'" + nl 
        + getAndClausesSql();
  }

  /**
   * subclasses provide AND clauses specific to their type
   * @return
   */
  public abstract String getAndClausesSql();

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
