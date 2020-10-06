package org.veupathdb.service.edass.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.gusdb.fgputil.functional.TreeNode;

public class StudySubsettingUtilsTest {
    
  private static TestModel model;
  
  private static final String nl = System.lineSeparator();

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
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(model.obsWeightFilter);
    filters.add(model.houseRoofFilter);
    
    List<String> entityIdsInFilters = StudySubsettingUtils.getEntityIdsInFilters(filters);

    assertEquals(2, entityIdsInFilters.size(), "ID set has incorrect size");
    assertTrue(entityIdsInFilters.contains(model.observation.getEntityId()), "ID set does not contain observ.");
    assertTrue(entityIdsInFilters.contains(model.household.getEntityId()), "ID set does not contain household.");
  }
  
  @Test
  @DisplayName("Test pruning an entity tree using 1 filter below")
  void testPruning1() {
    
    // create filter set with obs filter
    List<Filter> filters = new ArrayList<Filter>();
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
    List<Filter> filters = new ArrayList<Filter>();
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
    
    List<Filter> filters = new ArrayList<Filter>();
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

  @Test
  @DisplayName("Test creating a WITH clause without any relevant filters")
  void testWithClauseNoFilters() {
    
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(model.obsWeightFilter);
    filters.add(model.obsFavNewYearsFilter);
    String withClause = StudySubsettingUtils.generateWithClause(model.householdObs, filters);
    String expectedWithClause = "HouseholdObs as (" + nl +
        "SELECT household_obs_id FROM HouseObs_ancestors" + nl +
        ")";
    assertEquals(expectedWithClause, withClause);
  }

  @Test
  @DisplayName("Test creating a WITH clause with filters")
  void testWithClause() {
    
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(model.obsWeightFilter);
    filters.add(model.obsFavNewYearsFilter);
    filters.add(model.obsBirthDateFilter);
    filters.add(model.obsMoodFilter);
    filters.add(model.obsFavNumberFilter);
    filters.add(model.houseRoofFilter);
    String withClause = StudySubsettingUtils.generateWithClause(model.observation, filters);
    String expectedWithClause = "Observation as (" + nl + 
        "  SELECT observation_id FROM Obs_tall" + nl + 
        "  WHERE ontology_term_name = '12'" + nl + 
        "  AND number_value >= 10 AND number_value <= 20" + nl + 
        "INTERSECT" + nl + 
        "  SELECT observation_id FROM Obs_tall" + nl + 
        "  WHERE ontology_term_name = '15'" + nl + 
        "  AND date_value IN ('2019-03-21T00:00', '2019-03-28T00:00', '2019-06-12T00:00')" + nl + 
        "INTERSECT" + nl + 
        "  SELECT observation_id FROM Obs_tall" + nl + 
        "  WHERE ontology_term_name = '14'" + nl + 
        "  AND date_value >= '2019-03-21T00:00' AND date_value <= '2019-03-28T00:00'" + nl + 
        "INTERSECT" + nl + 
        "  SELECT observation_id FROM Obs_tall" + nl + 
        "  WHERE ontology_term_name = '16'" + nl + 
        "  AND string_value IN ('happy', 'jolly', 'giddy')" + nl + 
        "INTERSECT" + nl + 
        "  SELECT observation_id FROM Obs_tall" + nl + 
        "  WHERE ontology_term_name = '13'" + nl + 
        "  AND number_value IN (5, 7, 9 )" + nl + 
        ")";
    assertEquals(expectedWithClause, withClause);
  }

  @Test
  @DisplayName("Test creating a select clause for tabular report")
  void testGenerateTabularSelectClause() {
    
    String selectClause = StudySubsettingUtils.generateTabularSelectClause(model.observation);
    String expectedSelectClause = "SELECT " + model.household.getEntityFullPKColName() +
        ", " + model.participant.getEntityFullPKColName() +
        ", " + model.observation.getEntityFullPKColName() +
        ", string_value, number_value, date_value";
    assertEquals(expectedSelectClause, selectClause);
  }

  @Test
  @DisplayName("Test getting full ancestor PKs list")
  void testGetFullAncestorPKs() {
    Set<String> cols = new HashSet<String>(model.observation.getAncestorFullPkColNames());
    Set<String> expected = new HashSet<String>(Arrays.asList(new String[]{model.household.getEntityFullPKColName(), model.participant.getEntityFullPKColName()}));
    assertEquals(expected, cols);
  }
  
  @Test
  @DisplayName("Test populating ancestors")
  void testPopulateAncestors() {
    Entity e = model.study.getEntity(model.household.getEntityId());
    assertEquals(new ArrayList<Entity>(), e.getAncestorEntities());
    
    e = model.study.getEntity(model.participant.getEntityId());
    List<Entity> l = Arrays.asList(new Entity[]{model.household});
    assertEquals(l, e.getAncestorEntities());

    e = model.study.getEntity(model.householdObs.getEntityId());
    l = Arrays.asList(new Entity[]{model.household});
    assertEquals(l, e.getAncestorEntities());

    e = model.study.getEntity(model.observation.getEntityId());
    l = Arrays.asList(new Entity[]{model.household, model.participant});
    assertEquals(l, e.getAncestorEntities());
  
    e = model.study.getEntity(model.sample.getEntityId());
    l = Arrays.asList(new Entity[]{model.household, model.participant, model.observation});
    assertEquals(l, e.getAncestorEntities());
  
    e = model.study.getEntity(model.treatment.getEntityId());
    l = Arrays.asList(new Entity[]{model.household, model.participant, model.observation});
    assertEquals(l, e.getAncestorEntities());
  
  }
  
  @Test
  @DisplayName("Test creating a where clause for tabular report")
  void testGenerateTabularWhereClause() {
    
    List<String> vars = Arrays.asList(new String[]{model.birthDate.getId(), model.favNumber.getId()});
    String where = StudySubsettingUtils.generateTabularWhereClause(vars);
    String expected = "WHERE (\n" + 
        "  ontology_term_name = '14' OR" + nl +
        "  ontology_term_name = '13'" + nl +
        ")";

    assertEquals(expected, where);
  }

  @Test
  @DisplayName("Test creating an IN clause")
  void testGenerateInClauseClause() {
    
    // construct pruned tree with a pivot (H, HO, O)
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(model.obsWeightFilter);
    Entity outputEntity = model.householdObs;
    TreeNode<Entity> prunedTree = StudySubsettingUtils.pruneTree(model.study.getEntityTree(), filters, outputEntity);

    List<String> from = Arrays.asList(new String[]{model.household.getEntityName(), model.householdObs.getEntityName(), model.observation.getEntityName()});
    String inClause = StudySubsettingUtils.generateInClause(prunedTree, outputEntity);
    String expected = "AND " + model.householdObs.getEntityFullPKColName() + " IN (" + nl +
        "  SELECT " + model.householdObs.getEntityFullPKColName() +  nl +
        "  FROM " + String.join(", ", from) + nl +
        "  WHERE " + model.household.getEntityFullPKColName() + " = " + model.householdObs.getEntityName() + "." + model.household.getEntityPKColName() + nl +
        "  AND " + model.household.getEntityFullPKColName() + " = " + model.observation.getEntityName() + "." + model.household.getEntityPKColName() + nl +
        ")";

    assertEquals(expected, inClause);
  }

}
