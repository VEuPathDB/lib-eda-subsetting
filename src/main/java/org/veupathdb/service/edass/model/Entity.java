/**
 * 
 */
package org.veupathdb.service.edass.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Steve
 *
 */
public class Entity {
  private String id;
  private String name;
  private String description;
  private String tallTableName;
  private String ancestorsTableName;
  private String primaryKeyColumnName;
  private Map<String, Variable> variablesMap = new HashMap<String, Variable>();
  private List<Variable> variablesList = new ArrayList<Variable>();
  private List<Entity> ancestorEntities;
  private List<String> ancestorPkColNames;
  private List<String> ancestorFullPkColNames; // entityName.pkColName
  private Integer tallRowSize; // number of columns in a tall table row
  
  public Entity(String entityName, String entityId, String description, String entityTallTableName, String entityAncestorsTableName,
      String entityPrimaryKeyColumnName) {
    this.id = entityId;
    this.name = entityName;
    this.description = description;
    this.tallTableName = entityTallTableName;
    this.ancestorsTableName = entityAncestorsTableName;
    this.primaryKeyColumnName = entityPrimaryKeyColumnName;
  }
  
  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getTallTableName() {
    return tallTableName;
  }

  public String getPKColName() {
    return primaryKeyColumnName;
  }
  
  public String getFullPKColName() {
    return name + "." + primaryKeyColumnName;
  }
  
  public String getParentTableName() {
    return ancestorsTableName;
  }
  
  public List<String> getAncestorPkColNames() {
    return Collections.unmodifiableList(ancestorPkColNames);
  }
  
  public Optional<Variable> getVariable(String variableId) {
    return Optional.ofNullable(variablesMap.get(variableId));
  }
  
  public List<String> getAncestorFullPkColNames() {
    return Collections.unmodifiableList(ancestorFullPkColNames);
  }
  
  public void setAncestorEntities(List<Entity> ancestorEntities) {
    this.ancestorEntities = new ArrayList<Entity>(ancestorEntities);
    this.ancestorPkColNames = 
        ancestorEntities.stream().map(entry -> entry.getPKColName()).collect(Collectors.toList());
    this.ancestorFullPkColNames = 
        ancestorEntities.stream().map(entry -> entry.getName() + "." + entry.getPKColName()).collect(Collectors.toList());
  }

  public List<Entity> getAncestorEntities() {
    return Collections.unmodifiableList(ancestorEntities);
  }
  
  public String getEntityAncestorsTableName() {
    return ancestorsTableName;
  }

  public String toString() {
    return "id: " + getId() + " name: " + getName() + " (" + super.toString() + ")";
  }
  
  public String getAllPksSelectList(String entityTableName, String ancestorTableName) {
    List<String> selectColsList = new ArrayList<String>();
    for (String name : getAncestorPkColNames()) selectColsList.add(ancestorTableName + "." + name);
  
    selectColsList.add(entityTableName + "." + getPKColName());
    return String.join(", ", selectColsList);
  }
  
  // ancestor PKs, pk, variable_id, int_value, str_value, date_value
  Integer getTallRowSize() {
    if (tallRowSize == null) tallRowSize = Integer.valueOf(ancestorEntities.size() + 5);
    return tallRowSize;
  }
  
  public List<Variable> getVariables() {
    return Collections.unmodifiableList(variablesList);
  }
 
  public void addVariable(Variable var) {
    if (variablesMap.containsKey(var.getId()))
      throw new RuntimeException("Trying to add duplicate variable: " + var.getId());
    variablesMap.put(var.getId(), var);
    variablesList.add(var);
  }
}
