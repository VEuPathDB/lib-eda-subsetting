
package org.veupathdb.service.eda.subset.model;

import jakarta.ws.rs.BadRequestException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.veupathdb.service.eda.subset.model.varcollection.VarCollection;
import org.veupathdb.service.eda.subset.model.variable.Variable;
import org.veupathdb.service.eda.subset.model.variable.VariableDisplayType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Steve
 */
public class Entity {

  private static final Logger LOG = LogManager.getLogger(Entity.class);

  private final String id;
  private final String studyAbbrev; // internal abbrev
  private final String displayName;
  private final String displayNamePlural;
  private final String description;
  private final String abbreviation;

  private final Map<String, Variable> variablesMap = new HashMap<>();
  private final List<Variable> variablesList = new ArrayList<>();
  private List<Entity> ancestorEntities;
  private List<String> ancestorPkColNames;
  private List<String> ancestorFullPkColNames; // entityName.pkColName
  private Integer tallRowSize; // number of columns in a tall table row
  private boolean attributesTableExists;
  private final boolean hasCollections;
  private final boolean isManyToOneWithParent;

  private final List<VarCollection<?, ?>> collections = new ArrayList<>();

  // a map from ID of multifilter ancestor ID to multifilter leaf IDs
  // used to validate multifilter requests
  private final Map<String, Set<String>> _multiFilterMap = new HashMap<>();

  public Entity(String entityId, String studyAbbrev, String displayName, String displayNamePlural, String description, String abbreviation, boolean hasCollections, boolean isManyToOneWithParent) {
    this.id = entityId;
    this.studyAbbrev = studyAbbrev;
    this.displayName = displayName;
    this.displayNamePlural = displayNamePlural;
    this.description = description;
    this.abbreviation = abbreviation;
    this.hasCollections = hasCollections;
    this.isManyToOneWithParent = isManyToOneWithParent;
  }

  public String getAbbreviation() {
    return abbreviation;
  }

  public String getId() {
    return id;
  }

  public String getStudyAbbrev() {
    return studyAbbrev;
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

  public boolean isManyToOneWithParent() {
    return isManyToOneWithParent;
  }

  /**
   * @return whether this entity's hasCollections flag is set to true (in entity DB table)
   */
  public boolean hasCollections() {
    return hasCollections;
  }

  public void assignCollections(List<VarCollection<?, ?>> collections) {
    this.collections.clear();
    this.collections.addAll(collections);
  }

  public List<VarCollection<?, ?>> getCollections() {
    return collections;
  }

  public String getPKColName() {
    return getAbbreviation() + "_stable_id";
  }

  public String getPKColDotNotation() {
    return getId() + "." + getAbbreviation() + "_stable_id";
  }

  public String getFullPKColName() {
    return getWithClauseName() + "." + getPKColName();
  }

  public String getDownloadPkColHeader() {
    return getDisplayName().replace(' ','_') + "_ID";
  }

  public String getWithClauseName() {
    return id;
  }

  public Map<String, Set<String>> getMultiFilterMap() {
    return Collections.unmodifiableMap(_multiFilterMap);
  }

  /**
   * @return primary key column names of this entity's ancestors,
   * ordered by parent first, then grandparent, etc. until the root entity
   */
  public List<String> getAncestorPkColNames() {
    return Collections.unmodifiableList(ancestorPkColNames);
  }

  public Optional<Variable> getVariable(String variableId) {
    return Optional.ofNullable(variablesMap.get(variableId));
  }

  public Variable getVariableOrThrow(String variableId) {
    return getVariable(variableId).orElseThrow(
        () -> new BadRequestException("Variable '" + variableId + "' is not found"));
  }

  public List<String> getAncestorFullPkColNames() {
    return Collections.unmodifiableList(ancestorFullPkColNames);
  }

  public void setAttributesTableExists(boolean attributesTableExists) {
    this.attributesTableExists = attributesTableExists;
  }

  public boolean isAttributesTableExists() {
    return attributesTableExists;
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

  public String getAllPksSelectList(String ancestorTableName) {
    List<String> selectColsList = new ArrayList<>();
    selectColsList.add(ancestorTableName + "." + getPKColName());
    for (String name : getAncestorPkColNames())
      selectColsList.add(ancestorTableName + "." + name);
    return String.join(", ", selectColsList);
  }

  // ancestor PKs, pk, variable_id, value
  public Integer getTallRowSize() {
    if (tallRowSize == null) tallRowSize = ancestorEntities.size() + 3;
    return tallRowSize;
  }

  public boolean hasGeographicData() {
    return variablesList.stream().anyMatch(Variable::hasGeographicData);
  }

  public List<Variable> getVariables() {
    return Collections.unmodifiableList(variablesList);
  }

  public void addVariable(Variable var) {
    if (variablesMap.containsKey(var.getId()))
      throw new RuntimeException("In Entity '" + getId() + "', trying to add duplicate variable: " + var.getId());
    variablesMap.put(var.getId(), var);
    variablesList.add(var);
  }

  public void assignVariables(List<Variable> variables) {

    // create temporary map of parent IDs to child variables
    // use it  populate a concise map of multifilter ancestor IDs to leaf variables IDs
    Map<String, Set<Variable>> parentIdToKids = new HashMap<>();

    for (Variable var : variables) {
      addVariable(var);
      addToParentIdMap(parentIdToKids, var);
    }

    populateMultiFilterMap(parentIdToKids, _multiFilterMap);
  }

  private void addToParentIdMap(Map<String, Set<Variable>> parentIdToKids, Variable var) {
    String parentId = var.getParentId();
    if (parentId != null) {
      if (!parentIdToKids.containsKey(parentId)) parentIdToKids.put(parentId, new HashSet<>());
      parentIdToKids.get(parentId).add(var);
    }
  }

  /**
   * populate a map of multifilter ancestor IDs to leaf variables IDs
   * @param parentIdToKids -- map of variable ID to that variable's children
   */
  void populateMultiFilterMap(Map<String, Set<Variable>> parentIdToKids,
      Map<String, Set<String>> multiFilterMap) {

    // for any IDs that are multifilter, add to the multifilter map
    for (String parentId : parentIdToKids.keySet()) {
      if (!variablesMap.containsKey(parentId)) continue;  // if parent is of different entity

      if (variablesMap.get(parentId).getDisplayType() == VariableDisplayType.MULTIFILTER) {
        multiFilterMap.put(parentId, new HashSet<>());
        addToMultiFilterMap(parentId, parentId, parentIdToKids, multiFilterMap);
      }
    }
  }

  /**
   * recursively add value-carrying variables to their multifilter ancestor
   * @param multiFilterId - the ID of the variable tagged as 'multifilter'
   * @param nodeId - a descendant of the multifilter variable
   * @param parentIdToKids - a general map of parent ID to kid variables
   * @param multiFilterMap - the map to add to.  multifilter ID -> value-variable ID
   */
  void addToMultiFilterMap(String multiFilterId, String nodeId, Map<String,
      Set<Variable>> parentIdToKids, Map<String, Set<String>> multiFilterMap) {

    if (!parentIdToKids.containsKey(nodeId)) return;  // if node is not a parent, done with recursion

    for (Variable kid : parentIdToKids.get(nodeId)) {
      if (kid.hasValues()) multiFilterMap.get(multiFilterId).add(kid.getId());
      addToMultiFilterMap(multiFilterId, kid.getId(), parentIdToKids, multiFilterMap);
    }
  }

}
