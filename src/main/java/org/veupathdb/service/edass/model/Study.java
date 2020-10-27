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

import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.edass.generated.model.APIFilter;

public class Study {
  private String studyId;
  private TreeNode<Entity> entityTree;
  private Map<String, Entity> entityIdMap;
  private Map<String, Entity> variableIdToEntityMap;
  
  public Study(String studyId, TreeNode<Entity> entityTree, List<Variable> variables, Map<String, Entity> entityIdMap) {
    this.studyId = studyId;
    this.entityTree = entityTree;
    this.entityIdMap = entityIdMap;
    initEntitiesAndVariables(entityTree, variables);
  }
  
  /* 
   * Expects a pre-validated study ID
   */
  public static Study loadStudy(DataSource datasource, String studyId) {
    
    TreeNode<Entity> entityTree = EntityResultSetUtils.getStudyEntityTree(datasource, studyId);
    
    Map<String, Entity> entityIdMap = entityTree.flatten().stream().collect(Collectors.toMap(e -> e.getId(), e -> e)); 

    List<Variable> variables = VariableResultSetUtils.getStudyVariables(datasource, studyId, entityIdMap);

    return new Study(studyId, entityTree, variables, entityIdMap);
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
  
  /*
   * return true if valid study id
   */
  public static boolean validateStudyId(DataSource datasource, String studyId) {
    // TODO
    return false;
  }
  
  /**
   * Build internal (convenience) state from the raw entity tree 
   * @param rootEntityNode
   * @param vars
   */
  void initEntitiesAndVariables(TreeNode<Entity> rootEntityNode, List<Variable> vars) {
    initEntities(rootEntityNode);
    initVariables(vars);
  }
  
  /**
   * Build internal (convenience) state from the raw variables set
   * @param rootEntityNode
   * @param vars
   */
  void initEntities(TreeNode<Entity> rootEntityNode) {
    entityTree = rootEntityNode;
    validateEntityTreeIds(rootEntityNode);
    
    // give each entity a set of its ancestor entities.
    populateEntityAncestors(rootEntityNode);
  }  
  
  /**
   * Build internal (convenience) state from the raw entity tree and variables set
   * @param rootEntityNode
   * @param vars
   */
  void initVariables(List<Variable> vars) {
    variableIdToEntityMap = new HashMap<String, Entity>();    
    for (Variable var : vars) {
      variableIdToEntityMap.put(var.getId(), entityIdMap.get(var.getEntityId()));
      var.getEntity().addVariable(var);
    }
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
    String errPrefix = "In entity " + entityNode.getContents().getId() +
        ", found a child with the same ID as ";

    Set<String> childEntityIds = new HashSet<String>();
    for (TreeNode<Entity> child : entityNode.getChildNodes()) {
      Entity childEntity = child.getContents();
      if (childEntity.equals(entityNode.getContents().getId()))
        throw new RuntimeException(errPrefix + "the parent: " + childEntity.getId());

      if (childEntityIds.contains(childEntity.getId()))
        throw new RuntimeException(errPrefix + "another child: " + childEntity.getId());
    }
  }

}
