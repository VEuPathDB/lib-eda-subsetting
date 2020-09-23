package org.veupathdb.service.edass.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import javax.ws.rs.InternalServerErrorException;

import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.edass.generated.model.APIFilter;


public class Study {
  private String studyId;;
  private TreeNode<Entity> entityTree;
  private Map<String, Entity> entityIdMap = new HashMap<String, Entity>();
  private Set<Variable> variables;
  private Map<String, Entity> variableIdToEntityMap = new HashMap<String, Entity>();
  
  public Study(String studyId) {
    this.studyId = studyId;
  }
  
  public void initializeStudy(DataSource datasource) {
    validateStudyId(datasource, studyId);
    entityTree = loadEntityTree(datasource, studyId, entityIdMap);
    // TODO give each entity its list of ancestor pkCols
    variables = loadVariables(datasource, studyId);
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
  
  private Set<Variable> loadVariables(DataSource datasource, String studyId) {
    return null;
  }

  private void validateStudyId(DataSource datasource, String studyId) {
    
  }
  
  /*
   * get the full entity tree for this study from the datasource.
   * while we are at it, fill in the entity id map
   */
  private TreeNode<Entity> loadEntityTree(DataSource datasource, 
      String studyId, Map<String, Entity> entityIdMap) {
    // TODO
    return null;
  }
  
  /*
   * Confirm that a proposed child does not conflict with existing children.
   * Throw runtime exception if invalid
   */
  private static void validateChild(Entity child, TreeNode<Entity> parentNode) {
    String errPrefix = "In entity " + parentNode.getContents().getEntityId() + ", trying to add a child with the same ID as ";

    if (child.getEntityId().equals(parentNode.getContents().getEntityId()))
      throw new InternalServerErrorException(errPrefix + "the parent: " + child.getEntityId());

    if (parentNode.getChildNodes().stream()
        .filter(ch -> ch.getContents().getEntityId() == child.getEntityId()).count() != 0) 
      throw new InternalServerErrorException(errPrefix + "another child: " + child.getEntityId());
  }  

  
  public Entity getEntity(String entityId) {
    return entityIdMap.get(entityId);
  }
 
  public TreeNode<Entity> getEntityTree() {
    return entityTree.clone();
  }
}
