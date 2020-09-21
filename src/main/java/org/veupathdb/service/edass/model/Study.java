package org.veupathdb.service.edass.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.sql.DataSource;
import javax.ws.rs.InternalServerErrorException;

import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.edass.generated.model.APIDateRangeFilter;
import org.veupathdb.service.edass.generated.model.APIDateSetFilter;
import org.veupathdb.service.edass.generated.model.APIFilter;
import org.veupathdb.service.edass.generated.model.APINumberRangeFilter;
import org.veupathdb.service.edass.generated.model.APINumberSetFilter;
import org.veupathdb.service.edass.generated.model.APIStringSetFilter;

public class Study {
  private String studyId;;
  private TreeNode<Entity> fullStudyEntitiesTree;
  private Map<String, Entity> entityIdMap = new HashMap<String, Entity>();
  
  public Study(String studyId) {
    this.studyId = studyId;
  }
  
  public void initializeStudy(DataSource datasource) {
    validateStudyId(datasource, studyId);
    fullStudyEntitiesTree = getFullEntityTree(datasource, studyId, entityIdMap); 
    
  }

  public void produceSubset(DataSource datasource, String outputEntityId, Set<String> outputVariables,
      Set<APIFilter> filters) {
    
    Set<Filter> subsetFilters = constructSubsetFilters(filters);
    
    // TODO
    Predicate<Entity> isActive = e -> true;
    TreeNode<Entity> prunedEntityTree = trimToActiveAndPivotNodes(fullStudyEntitiesTree, isActive);
    /*
     * subsetReport = new subsetReport(entityTree, request.getFilters(), request.getOutputVariableIds());
     * subsetReport.validate(datasource);  
     * subsetReport.report(datasource);
     */
  }
  
  /*
   * PRUNE THE COMPLETE TREE TO JUST THE "ACTIVE" ENTITIES WE WANT FOR OUR JOINS
   * 
   * definition: an active entity is one that must be included in the SQL
   * definition: an active subtree is one in which any entities in the subtree are active.
   * 
   * this entity is active if any of these apply:
   *   1. it has filters
   *   2. it is the output entity
   *   3. it is neither of the above, but has more than one child that is the root of an active subtree
   *   
   *   (criterion 3 lets us join elements across connected subtrees)
   *   
   *          ----X----
   *          |       |
   *        --I--     I
   *        |   |     |
   *        A   I     A
   *        
   * In the picture above the A entities are active and I are inactive.  
   * X has two children that are active subtrees.
   * We need to force X to be active so that we can join the lower A entities.
   *
   * So will we now have this:
   * 
   *          ----A----
   *          |       |
   *        --I--     I
   *        |   |     |
   *        A   I     A
   *        
   * Finally, we want to prune the tree of inactive nodes, so we have the minimal active tree:
   * 
   *        ----A----
   *        |       |
   *        A       A
   *        
   * Now we can ascend the tree and form the concise SQL joins we need
   * 
   * Using a concrete example:
   *         ----H----
   *         |       |
   *       --P--     E
   *       |   |     |
   *       O   S     T
   * 
   * If O and and T are active (have filters or are the output entity), then we ultimately need this join:
   *   where O.H_id = H.H_id
   *     and T.H_id = H.H_id
   * 
   * (The graceful implementation below is courtesy of Ryan)
   */

  private static <T> TreeNode<T> trimToActiveAndPivotNodes(TreeNode<T> root, Predicate<T> isActive) {
    return root.mapStructure((nodeContents, mappedChildren) -> {
      List<TreeNode<T>> activeChildren = mappedChildren.stream()
        .filter(child -> child != null) // filter dead branches
        .collect(Collectors.toList());
      return isActive.test(nodeContents) || activeChildren.size() > 1 ?
        // this node is active itself or a pivot node; return with any active children
        new TreeNode<T>(nodeContents).addAllChildNodes(activeChildren) :
        // inactive, non-pivot node; return single active child or null
        activeChildren.isEmpty() ? null : activeChildren.get(0);
    });
  }
  
  private void validateStudyId(DataSource datasource, String studyId) {
    
  }
  
  /*
   * get the full entity tree for this study from the datasource.
   * while we are at it, fill in the entity id map
   */
  private TreeNode<Entity> getFullEntityTree(DataSource datasource, 
      String studyId, Map<String, Entity> entityIdMap) {
    // TODO
    return null;
  }
  
  private Set<Filter> constructSubsetFilters(Set<APIFilter> filters) {
   Set<Filter> subsetFilters = new HashSet<Filter>();
   
    for (APIFilter filter : filters) {
      
      Entity Entity = entityIdMap.get(filter.getEntityId());
      String pkCol = Entity.getEntityPrimaryKeyColumnName();
      String table = Entity.getEntityTallTableName();
      
      Filter newFilter;
      if (filter instanceof APIDateRangeFilter)
        newFilter = new DateRangeFilter((APIDateRangeFilter)filter, pkCol, table);   
      else if (filter instanceof APIDateSetFilter)
        newFilter = new DateSetFilter((APIDateSetFilter)filter, pkCol, table);
      else if (filter instanceof APINumberRangeFilter)
        newFilter = new NumberRangeFilter((APINumberRangeFilter)filter, pkCol, table);   
      else if (filter instanceof APINumberSetFilter)
        newFilter = new NumberSetFilter((APINumberSetFilter)filter, pkCol, table);
      else if (filter instanceof APIStringSetFilter)
        newFilter = new StringSetFilter((APIStringSetFilter)filter, pkCol, table);
      else 
        throw new InternalServerErrorException("Input filter not an expected subclass of Filter");

      subsetFilters.add(newFilter);   
    }
    return subsetFilters;
  } 
}
