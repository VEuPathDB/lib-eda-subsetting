package org.veupathdb.service.edass.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import static org.junit.jupiter.api.Assertions.*;

import org.veupathdb.service.edass.generated.model.APIDateRangeFilter;
import org.veupathdb.service.edass.generated.model.APIDateRangeFilterImpl;
import org.veupathdb.service.edass.generated.model.APIFilter;
import org.veupathdb.service.edass.generated.model.APIFilterImpl;
import org.veupathdb.service.edass.generated.model.APINumberRangeFilter;
import org.veupathdb.service.edass.generated.model.APINumberRangeFilterImpl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.InternalServerErrorException;

import org.gusdb.fgputil.functional.TreeNode;

public class StudySubsettingUtilsTest {
    
  private static TestModel model;
  
  @BeforeAll
  public static void setUp() {
    model = new TestModel();
  }

  
  /*
  static String getSqlJoinString(Entity parentEntity, Entity childEntity) {
    return parentEntity.getEntityName() + "." + parentEntity.getEntityPrimaryKeyColumnName() + " = " +
        childEntity.getEntityName() + "." + childEntity.getEntityPrimaryKeyColumnName();
  }
   */

  @Test
  @DisplayName("Test getting set of entity IDs from set of filters ")
  void testGetEntityIdsInFilters() {
   
    // add it to a set
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(model.obsWeightFilter);
    filters.add(model.houseRoofFilter);
    
    Set<String> entityIdsInFilters = StudySubsettingUtils.getEntityIdsInFilters(filters);

    assertEquals(2, entityIdsInFilters.size(), "ID set has incorrect size");
    assertTrue(entityIdsInFilters.contains(model.observation.getEntityId()), "ID set does not contain observ.");
    assertTrue(entityIdsInFilters.contains(model.household.getEntityId()), "ID set does not contain household.");
  }
  
  @Test
  @DisplayName("Test pruning an entity tree using 1 filter below")
  void testPruning1() {
    
    // create filter set with obs filter
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(model.obsWeightFilter);
    
    // set output entity
    Entity outputEntity = model.household;
    
    // prune tree
    TreeNode<Entity> prunedTree = StudySubsettingUtils.pruneTree(model.study.getEntityTree(), filters, outputEntity);
    
     // construct expected tree
    TreeNode<Entity> expectedTree = new TreeNode<Entity>(model.household);
    expectedTree.addChild(model.observation);

    // compare
    assertTrue(compareEntityTrees(prunedTree, expectedTree));
  }
  
  @Test
  @DisplayName("Test pruning an entity tree using 1 filter above")
  void testPruning2() {
    
    // add household roof filter to set
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(model.houseRoofFilter);
    
    // set output entity
    Entity outputEntity = model.observation;
    
    // prune tree
    TreeNode<Entity> prunedTree = StudySubsettingUtils.pruneTree(model.study.getEntityTree(), filters, outputEntity);
    
     // construct expected tree
    TreeNode<Entity> expectedTree = new TreeNode<Entity>(model.household);
    expectedTree.addChild(model.observation);

    // compare
    assertTrue(compareEntityTrees(prunedTree, expectedTree));
  }
  
  @Test
  @DisplayName("Test pruning an entity tree with a pivot")
  void testPruning3() {
    
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(model.obsWeightFilter);
    
    // set output entity
    Entity outputEntity = model.householdObs;
    
    // prune tree
    TreeNode<Entity> prunedTree = StudySubsettingUtils.pruneTree(model.study.getEntityTree(), filters, outputEntity);
    
     // construct expected tree
    TreeNode<Entity> expectedTree = new TreeNode<Entity>(model.household);
    expectedTree.addChild(model.householdObs);
    expectedTree.addChild(model.observation);

    /*
    System.out.println("Expected Tree: " + expectedTree);
    System.out.println("Pruned Tree: " + prunedTree);
    */
    
    // compare
    assertTrue(compareEntityTrees(prunedTree, expectedTree));
  }
  
  static boolean compareEntityTrees(TreeNode<Entity> t1, TreeNode<Entity> t2) {
    if ((t1 == null && t2 != null) || (t1 != null && t2 == null)) return false;
    
    if (t1 == null && t2 == null) return true;
    
    if (!t1.getContents().getEntityId().equals(t2.getContents().getEntityId())) return false;
    
    List<TreeNode<Entity>> t1Kids = t1.getChildNodes();
    List<TreeNode<Entity>> t2Kids = t2.getChildNodes();
    
    if (t1Kids.size() != t2Kids.size()) return false;
    
    for (int i=0; i < t1Kids.size(); i++)
      if (!compareEntityTrees(t1Kids.get(i), t2Kids.get(i))) return false;
    
    return true;
  }
/*
  @Test
  void testWithClause() {
    generateWithClauses(prunedEntityTree, filters, entityIdsInFilters);
  }
*/
}
