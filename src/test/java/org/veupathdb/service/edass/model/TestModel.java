package org.veupathdb.service.edass.model;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.edass.model.Variable.Resolution;
import org.veupathdb.service.edass.model.Variable.VariableType;

/**
 * A class that holds a test model of a study and supporting objects
 * @author Steve
 *
 */
public class TestModel {

  // reusable study objects
  public Study study; 
  
  public Entity household;
  public Entity householdObs;
  public Entity participant;
  public Entity observation;
  public Entity sample;
  public Entity treatment;
  
  public Variable roof;
  public Variable shoesize;
  public Variable weight;
  
  public Filter obsWeightFilter;
  public Filter houseRoofFilter;
  
  public TestModel() {
    createTestEntities();
    study = new Study("555555", constructEntityTree(), constructVariables()); 
    createFilters();
  }
  
  private void createTestEntities() {
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
  
  private TreeNode<Entity> constructEntityTree() {
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
  
  private Set<Variable> constructVariables() {
    Set<Variable> vars = new HashSet<Variable>();
    roof = new Variable("roof", "10", household.getEntityId(), VariableType.STRING, Resolution.CATEGORICAL);
    shoesize = new Variable("shoesize", "11", participant.getEntityId(), VariableType.NUMBER, Resolution.CATEGORICAL);    
    weight = new Variable("weight", "12", observation.getEntityId(), VariableType.NUMBER, Resolution.CONTINUOUS);    
    vars.add(roof);
    vars.add(shoesize);
    vars.add(weight);
    return vars;
  }
  
  private void createFilters() {
    
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


}
