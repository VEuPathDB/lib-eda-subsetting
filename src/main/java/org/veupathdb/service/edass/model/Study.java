package org.veupathdb.service.edass.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;
import javax.ws.rs.InternalServerErrorException;

import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.edass.generated.model.APIFilter;


public class Study {
  private String studyId;
  private TreeNode<Entity> entityTree;
  private Map<String, Entity> entityIdMap;
  private Map<String, Variable> variablesMap;  // id -> Variable
  private Map<String, Entity> variableIdToEntityMap;
  
  public Study(String studyId, TreeNode<Entity> entityTree, Set<Variable> variables) {
    this.studyId = studyId;
    this.entityTree = entityTree;
    initEntitiesAndVariables(entityTree, variables);
  }
  
  /* 
   * Expects a pre-validated study ID
   */
  public static Study loadStudy(DataSource datasource, String studyId) {
    TreeNode<Entity> entityTree = loadEntityTree(datasource);
    Set<Variable> variables = loadVariables(datasource);
    return new Study(studyId, entityTree, variables);
  }
  
  /** 
   * 
   * @param entityId
   * @param variableIds
   * @return list of error messages, if any
   */
  // TODO
  public List<String> validateEntityVariables(String entityId, List<String> variableIds) {
    return null;
  }
  
  /**
   * 
   * @param apiFilters
   * @return list of error messages, if any
   */
  // TODO
  public List<String> validateApiFilters(List<APIFilter> apiFilters) {
    return null;
  }
  
  private static Set<Variable> loadVariables(DataSource datasource) {
    return null;
  }

  /*
   * return true if valid study id
   */
  public static boolean validateStudyId(DataSource datasource, String studyId) {
    return false;
  }
  
  /*
   * get the full entity tree for this study from the datasource.
   */
  private static TreeNode<Entity> loadEntityTree(DataSource datasource) {
    // TODO
    return null;
  }
  
  /**
   * Build internal (convenience) state from the raw entity tree and variables set
   * @param rootEntityNode
   * @param vars
   */
  void initEntitiesAndVariables(TreeNode<Entity> rootEntityNode, Set<Variable> vars) {
    entityTree = rootEntityNode;
    validateEntityTreeIds(rootEntityNode);
    
    // build entity ID map
    entityIdMap = rootEntityNode.flatten().stream().collect(Collectors.toMap(e -> e.getEntityId(), e -> e)); 

    // give each entity a set of its ancestor entities.
    populateEntityAncestors(rootEntityNode);
    
    variablesMap = new HashMap<String, Variable>();
    for (Variable var : vars) variablesMap.put(var.getId(), var);

    variableIdToEntityMap = new HashMap<String, Entity>();
    for (Variable var : vars)
      variableIdToEntityMap.put(var.getId(), entityIdMap.get(var.getEntityId()));
   }
  
  public String getStudyId() {
    return studyId;
  }

  public Optional<Entity> getEntity(String entityId) {
    return Optional.ofNullable(entityIdMap.get(entityId));
  }
   
  public TreeNode<Entity> getEntityTree() {
    return entityTree.clone();
  }
  
  public Optional<Variable> getVariable(String variableId) {
    return Optional.ofNullable(variablesMap.get(variableId));
  }
  
  private static void populateEntityAncestors(TreeNode<Entity> rootEntityNode) {
    populateEntityAncestors(rootEntityNode, new ArrayList<>());
  }
  
  private static void populateEntityAncestors(TreeNode<Entity> entityNode, List<Entity> ancestorEntities) {
    Entity entity = entityNode.getContents();
    entity.setAncestorEntities(ancestorEntities);
    ancestorEntities.add(entity);
    for (TreeNode<Entity> childNode : entityNode.getChildNodes()) {
      populateEntityAncestors(childNode, new ArrayList<Entity>(ancestorEntities));
    }
  }
  
  /*
   * Confirm that children have non-conflicting entity IDs. Throw runtime exception if invalid
   */
  private static void validateEntityTreeIds(TreeNode<Entity> entityNode) {
    String errPrefix = "In entity " + entityNode.getContents().getEntityId() +
        ", found a child with the same ID as ";

    Set<String> childEntityIds = new HashSet<String>();
    for (TreeNode<Entity> child : entityNode.getChildNodes()) {
      Entity childEntity = child.getContents();
      if (childEntity.equals(entityNode.getContents().getEntityId()))
        throw new InternalServerErrorException(errPrefix + "the parent: " + childEntity.getEntityId());

      if (childEntityIds.contains(childEntity.getEntityId()))
        throw new InternalServerErrorException(errPrefix + "another child: " + childEntity.getEntityId());
    }
  }

}
