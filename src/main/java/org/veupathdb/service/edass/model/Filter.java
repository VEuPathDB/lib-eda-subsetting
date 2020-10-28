package org.veupathdb.service.edass.model;

import static org.veupathdb.service.edass.model.RdbmsColumnNames.*;

public abstract class Filter {
  protected Entity entity;
  protected String variableId;
  
  public Filter(Entity entity, String variableId) {
    if (entity == null) throw new RuntimeException("Null entity not allowed");
    entity.getVariable(variableId).orElseThrow(() -> new RuntimeException("Entity " + entity.getId() + "does not contain variable " + variableId));
    this.entity = entity;
    this.variableId = variableId;
  }

  public String getSql() {
    return entity.getAncestorPkColNames().isEmpty()?
        getSqlNoAncestors() :
          getSqlWithAncestors();
  }

  private String getSqlWithAncestors() {

    return "  SELECT " + entity.getAllPksSelectList("t", "a") + nl 
        + "  FROM " + entity.getTallTableName() + " t, " + entity.getEntityAncestorsTableName() + " a" + nl
        + "  WHERE t." + entity.getPKColName() + " = a." + entity.getPKColName() + nl 
        + "  AND " + VARIABLE_ID_COL_NAME + " = '" + variableId + "'" + nl 
        + getAndClausesSql();
  }
  
  private String getSqlNoAncestors() {
    
    return "  SELECT " + entity.getPKColName() + nl 
        + "  FROM " + entity.getTallTableName() + nl
        + "  WHERE " + VARIABLE_ID_COL_NAME + " = '" + variableId + "'" + nl 
        + getAndClausesSql();
  }

  /**
   * subclasses provide AND clauses specific to their type
   * @return
   */
  public abstract String getAndClausesSql();

  public Entity getEntity() {
    return entity;
  }
  
  
}
