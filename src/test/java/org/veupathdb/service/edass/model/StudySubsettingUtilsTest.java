package org.veupathdb.service.edass.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.veupathdb.service.edass.generated.model.APIDateRangeFilter;
import org.veupathdb.service.edass.generated.model.APIDateRangeFilterImpl;
import org.veupathdb.service.edass.generated.model.APIFilter;
import org.veupathdb.service.edass.generated.model.APIFilterImpl;
import org.veupathdb.service.edass.generated.model.APINumberRangeFilter;
import org.veupathdb.service.edass.generated.model.APINumberRangeFilterImpl;
import org.veupathdb.service.edass.model.Variable.Resolution;
import org.veupathdb.service.edass.model.Variable.VariableType;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.InternalServerErrorException;

import org.gusdb.fgputil.functional.TreeNode;

public class StudySubsettingUtilsTest {
  
  // reusable study objects
  private static Study testStudy; 
  
  private static Entity household;
  private static Entity householdObs;
  private static Entity participant;
  private static Entity observation;
  private static Entity sample;
  private static Entity treatment;
  
  private static Variable roof;
  private static Variable shoesize;
  private static Variable weight;
  
  private static Filter obsWeightFilter;
  private static Filter houseRoofFilter;
  
  @BeforeAll
  public static void setUp() {
    testStudy = createTestStudy();
    createStaticFilters();
  }

  static Study createTestStudy() {
    Study study = new Study("555555"); 
    createTestEntities();
    TreeNode<Entity> entityTree = constructEntityTree();
    Set<Variable> variables = constructVariables();
    study.initEntitiesAndVariables(entityTree, variables);
    return study;
  }
  
  static void createTestEntities() {
    household = new Entity("Household", "1", "Hshld_tall", "Hshld_ancestors",
        "household_id");
    householdObs = new Entity("HouseholdObs", "4", "HouseObs_tall", "HouseObs_ancestors",
        "household_obs_id");
    participant = new Entity("Participant", "2", "Part_tall", "Part_ancestors",
        "participant_id");
    observation = new Entity("Observation", "3", "Obs_tall", "Obs_ancestors",
        "observation_id");
    sample = new Entity("Sample", "5", "Sample_tall", "Sample_ancestors",
        "sample_id");
    treatment = new Entity("Treatment", "6", "Treatment_tall", "Treatment_ancestors",
        "treatment_id");
  }
  
  static TreeNode<Entity> constructEntityTree() {
    TreeNode<Entity> householdNode = new TreeNode<Entity>(household);

    TreeNode<Entity> houseObsNode = new TreeNode<Entity>(householdObs);
    householdNode.addChildNode(houseObsNode);

    TreeNode<Entity> participantNode = new TreeNode<Entity>(participant);
    householdNode.addChildNode(participantNode);

    TreeNode<Entity> observationNode = new TreeNode<Entity>(observation);
    participantNode.addChildNode(observationNode);
    
    TreeNode<Entity> sampleNode = new TreeNode<Entity>(sample);
    observationNode.addChildNode(sampleNode);
    
    TreeNode<Entity> treatmentNode = new TreeNode<Entity>(treatment);
    observationNode.addChildNode(treatmentNode);
    
    return householdNode;
  }
  
  static Set<Variable> constructVariables() {
    Set<Variable> vars = new HashSet<Variable>();
    roof = new Variable("roof", "10", household.getEntityId(), VariableType.STRING, Resolution.CATEGORICAL);
    shoesize = new Variable("shoesize", "11", participant.getEntityId(), VariableType.NUMBER, Resolution.CATEGORICAL);    
    weight = new Variable("weight", "12", observation.getEntityId(), VariableType.NUMBER, Resolution.CONTINUOUS);    
    vars.add(roof);
    vars.add(shoesize);
    vars.add(weight);
    return vars;
  }
  
  static void createStaticFilters() {
    
    // create observation weight filter
    obsWeightFilter = new NumberRangeFilter(observation.getEntityId(),
        observation.getEntityPrimaryKeyColumnName(), observation.getEntityTallTableName(), 
        weight.getId(), 10, 20);

    // create household roof filter
    List<String> roofs = Arrays.asList(new String[]{"metal", "tile"});
    houseRoofFilter = new StringSetFilter(household.getEntityId(),
        household.getEntityPrimaryKeyColumnName(), household.getEntityTallTableName(),
        roof.getId(), roofs);
  }
  
  /*
  static String getSqlJoinString(Entity parentEntity, Entity childEntity) {
    return parentEntity.getEntityName() + "." + parentEntity.getEntityPrimaryKeyColumnName() + " = " +
        childEntity.getEntityName() + "." + childEntity.getEntityPrimaryKeyColumnName();
  }
   */

  @Test
  @DisplayName("Test valid construction of filters from API filters")
  void testConstructFilters() {
    
    Set<APIFilter> afs = new HashSet<APIFilter>();
    
    APIDateRangeFilter dateFilter = new APIDateRangeFilterImpl();
    dateFilter.setEntityId(participant.getEntityId());
    
    afs.add(dateFilter);
    
    APINumberRangeFilter numberFilter = new APINumberRangeFilterImpl();
    numberFilter.setEntityId(treatment.getEntityId());
    afs.add(numberFilter);
    
    StudySubsettingUtils.constructFiltersFromAPIFilters(testStudy, afs);
    
    assertEquals(2, afs.size());
  }

  @Test
  @DisplayName("Test rejection of invalid API filters")
  void testIncorrectConstructFilters() {
    
    assertThrows(InternalServerErrorException.class, new Executable() {
      
      @Override
      public void execute() throws Throwable {
        Set<APIFilter> afs = new HashSet<APIFilter>();
        
        // a legit filter
        APIDateRangeFilter dateFilter = new APIDateRangeFilterImpl();
        dateFilter.setEntityId(household.getEntityId());
        afs.add(dateFilter);
        
        // illegit... can't be the superclass
        APIFilter nakedFilter = new APIFilterImpl();
        nakedFilter.setEntityId(sample.getEntityId());
        afs.add(nakedFilter);

        StudySubsettingUtils.constructFiltersFromAPIFilters(testStudy, afs);
      }
    });
  }
  
  @Test
  @DisplayName("Test getting set of entity IDs from set of filters ")
  void testGetEntityIdsInFilters() {
   
    // add it to a set
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(obsWeightFilter);
    filters.add(houseRoofFilter);
    
    Set<String> entityIdsInFilters = StudySubsettingUtils.getEntityIdsInFilters(filters);

    assertEquals(2, entityIdsInFilters.size(), "ID set has incorrect size");
    assertTrue(entityIdsInFilters.contains(observation.getEntityId()), "ID set does not contain observ.");
    assertTrue(entityIdsInFilters.contains(household.getEntityId()), "ID set does not contain household.");
  }
  
  @Test
  @DisplayName("Test pruning an entity tree using 1 filter below")
  void testPruning1() {
    
    // create filter set with obs filter
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(obsWeightFilter);
    
    // set output entity
    Entity outputEntity = household;
    
    // prune tree
    TreeNode<Entity> prunedTree = StudySubsettingUtils.pruneTree(testStudy.getEntityTree(), filters, outputEntity);
    
     // construct expected tree
    TreeNode<Entity> expectedTree = new TreeNode<Entity>(household);
    expectedTree.addChild(observation);

    // compare
    assertTrue(compareEntityTrees(prunedTree, expectedTree));
  }
  
  @Test
  @DisplayName("Test pruning an entity tree using 1 filter above")
  void testPruning2() {
    
    // add household roof filter to set
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(houseRoofFilter);
    
    // set output entity
    Entity outputEntity = observation;
    
    // prune tree
    TreeNode<Entity> prunedTree = StudySubsettingUtils.pruneTree(testStudy.getEntityTree(), filters, outputEntity);
    
     // construct expected tree
    TreeNode<Entity> expectedTree = new TreeNode<Entity>(household);
    expectedTree.addChild(observation);

    // compare
    assertTrue(compareEntityTrees(prunedTree, expectedTree));
  }
  
  @Test
  @DisplayName("Test pruning an entity tree with a pivot")
  void testPruning3() {
    
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(obsWeightFilter);
    
    // set output entity
    Entity outputEntity = householdObs;
    
    // prune tree
    TreeNode<Entity> prunedTree = StudySubsettingUtils.pruneTree(testStudy.getEntityTree(), filters, outputEntity);
    
     // construct expected tree
    TreeNode<Entity> expectedTree = new TreeNode<Entity>(household);
    expectedTree.addChild(householdObs);
    expectedTree.addChild(observation);

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
