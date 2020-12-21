
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
  private String studyId;  // study's stable ID
  private String displayName;
  private String displayNamePlural;
  private String description;
  private String abbreviation;

  private Map<String, Variable> variablesMap = new HashMap<>();
  private List<Variable> variablesList = new ArrayList<>();
  private List<Entity> ancestorEntities;
  private List<String> ancestorPkColNames;
  private List<String> ancestorFullPkColNames; // entityName.pkColName
  private Integer tallRowSize; // number of columns in a tall table row
  
  public Entity(String entityId, String studyId, String displayName, String displayNamePlural, String description, String abbreviation) {
    this.id = entityId;
    this.studyId = studyId;
    this.displayName = displayName;
    this.displayNamePlural = displayNamePlural;
    this.description = description;
    this.abbreviation = abbreviation;
  }

  public String getAbbreviation() {
    return abbreviation;
  }

  public String getId() {
    return id;
  }

  public String getStudyId() {
    return studyId;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getDisplayNamePlural() {
    return displayNamePlural;
  }

  public String getDescription() {
    return description;
  }

  public String getTallTableName() {
    return "AttrVal_" + getStudyId() + "_" + getAbbreviation();
  }

  public String getPKColName() {
    return getAbbreviation() + "_id";
  }
  
  public String getFullPKColName() {
    return getWithClauseName() + "." + getPKColName();
  }
  
  public String getAncestorsTableName() {
    return "Ancestors_" + getStudyId() + "_" + getAbbreviation();
  }

  public String getWithClauseName() {
    return id;
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
    this.ancestorEntities = new ArrayList<>(ancestorEntities);
    this.ancestorPkColNames = 
        ancestorEntities.stream().map(Entity::getPKColName).collect(Collectors.toList());
    this.ancestorFullPkColNames = 
        ancestorEntities.stream().map(Entity::getFullPKColName).collect(Collectors.toList());
  }

  public List<Entity> getAncestorEntities() {
    return Collections.unmodifiableList(ancestorEntities);
  }
  
  public String toString() {
    return "id: " + getId() + " name: " + getDisplayName() + " (" + super.toString() + ")";
  }
  
  public String getAllPksSelectList(String entityTableName, String ancestorTableName) {
    List<String> selectColsList = new ArrayList<>();
    for (String name : getAncestorPkColNames()) selectColsList.add(ancestorTableName + "." + name);
  
    selectColsList.add(entityTableName + "." + getPKColName());
    return String.join(", ", selectColsList);
  }
  
  // ancestor PKs, pk, variable_id, value
  Integer getTallRowSize() {
    if (tallRowSize == null) tallRowSize = ancestorEntities.size() + 3;
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
