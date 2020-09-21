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
import org.veupathdb.lib.container.jaxrs.utils.db.DbManager;
import org.veupathdb.service.edass.generated.model.DateRangeFilter;
import org.veupathdb.service.edass.generated.model.DateSetFilter;
import org.veupathdb.service.edass.generated.model.Filter;
import org.veupathdb.service.edass.generated.model.NumberRangeFilter;
import org.veupathdb.service.edass.generated.model.NumberSetFilter;
import org.veupathdb.service.edass.generated.model.StringSetFilter;

public class SubsetStudy {
  private String studyId;;
  private TreeNode<SubsetEntity> fullStudyEntitiesTree;
  private Map<String, SubsetEntity> entityIdMap = new HashMap<String, SubsetEntity>();
  
  public SubsetStudy(String studyId) {
    this.studyId = studyId;
  }
  
  public void initializeStudy(DataSource datasource) {
    validateStudyId(datasource, studyId);
    fullStudyEntitiesTree = getFullEntityTree(datasource, studyId, entityIdMap); 
    
  }

  public void produceSubset(DataSource datasource, String outputEntityId, Set<String> outputVariables,
      Set<Filter> filters) {
    
    Set<SubsetFilter> subsetFilters = constructSubsetFilters(filters);
    Predicate<SubsetEntity> isActive = new Predicate<SubsetEntity>();
    
    Predicate<SubsetEntity> p1 = c -> c.getName().startsWith("I") && 
        c.getPopulation() > 10000000;
    TreeNode<SubsetEntity> prunedEntityTree = trimToActiveAndPivotNodes(fullStudyEntitiesTree, Predicate<T> isActive);
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
  private TreeNode<SubsetEntity> getFullEntityTree(DataSource datasource, 
      String studyId, Map<String, SubsetEntity> entityIdMap) {
    // TODO
    return null;
  }
  
  private Set<SubsetFilter> constructSubsetFilters(Set<Filter> filters) {
   Set<SubsetFilter> subsetFilters = new HashSet<SubsetFilter>();
   
    for (Filter filter : filters) {
      
      SubsetEntity subsetEntity = entityIdMap.get(filter.getEntityId());
      String pkCol = subsetEntity.getEntityPrimaryKeyColumnName();
      String table = subsetEntity.getEntityTallTableName();
      
      SubsetFilter newFilter;
      if (filter instanceof DateRangeFilter)
        newFilter = new SubsetDateRangeFilter((DateRangeFilter)filter, pkCol, table);   
      else if (filter instanceof DateSetFilter)
        newFilter = new SubsetDateSetFilter((DateSetFilter)filter, pkCol, table);
      else if (filter instanceof NumberRangeFilter)
        newFilter = new SubsetNumberRangeFilter((NumberRangeFilter)filter, pkCol, table);   
      else if (filter instanceof NumberSetFilter)
        newFilter = new SubsetNumberSetFilter((NumberSetFilter)filter, pkCol, table);
      else if (filter instanceof StringSetFilter)
        newFilter = new SubsetStringSetFilter((StringSetFilter)filter, pkCol, table);
      else 
        throw new InternalServerErrorException("Input filter not an expected subclass of Filter");

      subsetFilters.add(newFilter);   
    }
    return subsetFilters;
  }
  
  public class 
}
