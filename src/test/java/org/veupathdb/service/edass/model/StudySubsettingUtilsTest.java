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
import org.veupathdb.service.edass.generated.model.APIStringSetFilter;
import org.veupathdb.service.edass.generated.model.APIStringSetFilterImpl;
import org.veupathdb.service.edass.model.Variable.VariableType;

import static org.junit.jupiter.api.Assertions.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.InternalServerErrorException;

import org.gusdb.fgputil.functional.TreeNode;

public class StudySubsettingUtilsTest {
  
  private static Study testStudy;
  private static Entity household = new Entity("Household", "1", "Hshld_tall", "Hshld_ancestors",
      "household_id");
  private static Entity householdObs = new Entity("HouseholdObs", "4", "HouseObs_tall", "HouseObs_ancestors",
      "household_obs_id");
  private static Entity participant = new Entity("Participant", "2", "Part_tall", "Part_ancestors",
      "participant_id");
  private static Entity observation = new Entity("Observation", "3", "Obs_tall", "Obs_ancestors",
      "observation_id");
  private static Entity sample = new Entity("Sample", "5", "Sample_tall", "Sample_ancestors",
      "sample_id");
  private static Entity treatment = new Entity("Treatment", "6", "Treatment_tall", "Treatment_ancestors",
      "treatment_id");

  @BeforeAll
  public static void setUp() {
      testStudy = createTestStudy();
  }

  static Study createTestStudy() {
    Study study = new Study("555555"); 
    TreeNode<Entity> entityTree = constructEntityTree();
    Set<Variable> variables = constructVariables();
    study.initEntitiesAndVariables(entityTree, variables);
    return study;
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
    Variable v = new Variable("roof", "10", "1", VariableType.STRING);
    vars.add(v);
    return vars;
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
    dateFilter.setEntityId("5");
    afs.add(dateFilter);
    APINumberRangeFilter numberFilter = new APINumberRangeFilterImpl();
    numberFilter.setEntityId("3");
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
        dateFilter.setEntityId("5");
        afs.add(dateFilter);
        
        // illegit... can't be the superclass
        APIFilter nakedFilter = new APIFilterImpl();
        nakedFilter.setEntityId("3");
        afs.add(nakedFilter);

        StudySubsettingUtils.constructFiltersFromAPIFilters(testStudy, afs);
      }
    });
  }
  
  @Test
  void testGetEntityIdsInFilters() {
   
    // create observation filter
    APIStringSetFilter apiFilter = new APIStringSetFilterImpl();
    Filter obsFilter = new StringSetFilter(apiFilter, observation.getEntityId(),
        observation.getEntityPrimaryKeyColumnName(), observation.getEntityTallTableName());
    
    // create household filter
    apiFilter = new APIStringSetFilterImpl();
    Filter houseFilter = new StringSetFilter(apiFilter, household.getEntityId(),
        household.getEntityPrimaryKeyColumnName(), household.getEntityTallTableName());
    
    // add it to a set
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(obsFilter);
    filters.add(houseFilter);
    
    Set<String> entityIdsInFilters = StudySubsettingUtils.getEntityIdsInFilters(filters);

    assertEquals(2, entityIdsInFilters.size(), "ID set is correct size");
    assertTrue(entityIdsInFilters.contains(observation.getEntityId()), "ID set contains observ.");
    assertTrue(entityIdsInFilters.contains(household.getEntityId()), "ID set contains household.");
  }
  
  @Test
  void testPruning1() {
    
    // construct filter from API filter
    APIStringSetFilter apiFilter = new APIStringSetFilterImpl();
    Filter obsFilter = new StringSetFilter(apiFilter, observation.getEntityId(),
        observation.getEntityPrimaryKeyColumnName(), observation.getEntityTallTableName());
    
    // add it to a set
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(obsFilter);
    
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
  
/*
  @Test
  void testPruning() {
    Set<Filter> filters = new HashSet<Filter>();
    
    Entity outputEntity = participant;

    StudySubsettingUtils.pruneTree(testStudy.getEntityTree(), filters, outputEntity);
  }
 */
  
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


}
