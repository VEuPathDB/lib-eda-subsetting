package org.veupathdb.service.edass.model;

import static org.veupathdb.service.edass.model.RdbmsColumnNames.*;

public abstract class Filter {
  protected Entity entity;
  protected String variableName;
  
  public Filter(Entity entity, String variableName) {
    this.entity = entity;
    this.variableName = variableName;
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
        + "  AND " + VARIABLE_ID_COL_NAME + " = '" + variableName + "'" + nl 
        + getAndClausesSql();
  }
  
  private String getSqlNoAncestors() {
    
    return "  SELECT " + entity.getPKColName() + nl 
        + "  FROM " + entity.getTallTableName() + nl
        + "  WHERE " + VARIABLE_ID_COL_NAME + " = '" + variableName + "'" + nl 
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
