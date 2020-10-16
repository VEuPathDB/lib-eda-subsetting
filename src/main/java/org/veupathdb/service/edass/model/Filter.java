package org.veupathdb.service.edass.model;

public abstract class Filter {
  protected Entity entity;
  protected String variableName;
  
  protected static final String nl = System.lineSeparator();

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
        + "  AND ontology_term_name = '" + variableName + "'" + nl 
        + getAndClausesSql();
  }
  
  private String getSqlNoAncestors() {
    
    return "  SELECT " + entity.getPKColName() + nl 
        + "  FROM " + entity.getTallTableName() + nl
        + "  WHERE ontology_term_name = '" + variableName + "'" + nl 
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
