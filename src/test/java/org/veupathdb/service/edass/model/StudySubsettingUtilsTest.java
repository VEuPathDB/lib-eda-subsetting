package org.veupathdb.service.edass.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.edass.generated.model.APIDateRangeFilterImpl;
import org.veupathdb.service.edass.generated.model.APIFilter;
import org.veupathdb.service.edass.generated.model.APINumberSetFilterImpl;
import org.veupathdb.service.edass.model.Variable.VariableType;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.HashSet;
import java.util.Set;

import org.gusdb.fgputil.functional.TreeNode;

public class StudySubsettingUtilsTest {
  
  private static Study testStudy;
  
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
    Entity household = new Entity("Household", "1", "Hshld_tall", "Hshld_ancestors",
        "household_id");
    TreeNode<Entity> householdNode = new TreeNode<Entity>(household);

    Entity participant = new Entity("Participant", "2", "Part_tall", "Part_ancestors",
        "participant_id");
    TreeNode<Entity> participantNode = new TreeNode<Entity>(participant);
    householdNode.addChildNode(participantNode);

    Entity observation = new Entity("Observation", "3", "Obs_tall", "Obs_ancestors",
        "observation_id");
    TreeNode<Entity> observationNode = new TreeNode<Entity>(observation);
    participantNode.addChildNode(observationNode);
    
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
  void testConstructFilters() {
    Set<APIFilter> afs = new HashSet<APIFilter>();
    afs.add(new APIDateRangeFilterImpl());
    afs.add(new APINumberSetFilterImpl());
    StudySubsettingUtils.constructFiltersFromAPIFilters(testStudy, afs);
    assertEquals(2, afs.size());
  }

}
