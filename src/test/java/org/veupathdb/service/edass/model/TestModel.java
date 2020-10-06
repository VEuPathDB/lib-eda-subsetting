package org.veupathdb.service.edass.model;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
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
  public Variable favNumber;
  public Variable birthDate;
  public Variable favNewYears;
  public Variable mood;
  
  public Filter obsWeightFilter;
  public Filter houseRoofFilter;
  public Filter obsFavNumberFilter; // categorical numeric
  public Filter obsBirthDateFilter;  // continuous date
  public Filter obsFavNewYearsFilter;  // categorical numeric
  public Filter obsMoodFilter; // string 

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
  
  /*
   * return a fresh entity tree.
   */
  public TreeNode<Entity> constructEntityTree() {
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
    vars.add(roof);
    
    shoesize = new Variable("shoesize", "11", participant.getEntityId(), VariableType.NUMBER, Resolution.CATEGORICAL);    
    vars.add(shoesize);

    weight = new Variable("weight", "12", observation.getEntityId(), VariableType.NUMBER, Resolution.CONTINUOUS);    
    vars.add(weight);
    
    favNumber = new Variable("favNumber", "13", observation.getEntityId(), VariableType.NUMBER, Resolution.CATEGORICAL);    
    vars.add(weight);
    
    birthDate  = new Variable("birthDate", "14", observation.getEntityId(), VariableType.DATE, Resolution.CONTINUOUS);    
    vars.add(birthDate);
    
    favNewYears = new Variable("favNewYears", "15", observation.getEntityId(), VariableType.DATE, Resolution.CATEGORICAL);    
    vars.add(favNewYears);
    
    mood  = new Variable("mood", "16", observation.getEntityId(), VariableType.STRING, Resolution.CATEGORICAL);    
    vars.add(mood);

    return vars;
  }
  
  private void createFilters() {
    
    // create observation weight filter
    obsWeightFilter = new NumberRangeFilter(observation.getEntityId(),
        observation.getEntityPrimaryKeyColumnName(), observation.getEntityTallTableName(), 
        weight.getId(), 10, 20);

    List<Number> favNums = Arrays.asList(new Number[]{5,7,9});
    obsFavNumberFilter = new NumberSetFilter(observation.getEntityId(),
        observation.getEntityPrimaryKeyColumnName(), observation.getEntityTallTableName(), 
        favNumber.getId(), favNums); 

    obsBirthDateFilter = new DateRangeFilter(observation.getEntityId(),
        observation.getEntityPrimaryKeyColumnName(), observation.getEntityTallTableName(), 
        birthDate.getId(),
        LocalDateTime.of(2019, Month.MARCH, 21, 00, 00),
        LocalDateTime.of(2019, Month.MARCH, 28, 00, 00));

    List<LocalDateTime> dates = new ArrayList<LocalDateTime>();
    dates.add(LocalDateTime.of(2019, Month.MARCH, 21, 00, 00));
    dates.add(LocalDateTime.of(2019, Month.MARCH, 28, 00, 00));
    dates.add(LocalDateTime.of(2019, Month.JUNE, 12, 00, 00));
    obsFavNewYearsFilter = new DateSetFilter(observation.getEntityId(),
        observation.getEntityPrimaryKeyColumnName(), observation.getEntityTallTableName(), 
        favNewYears.getId(),
        dates);

    List<String> moods = Arrays.asList(new String[]{"happy", "jolly", "giddy"});
    obsMoodFilter = new StringSetFilter(observation.getEntityId(),
        observation.getEntityPrimaryKeyColumnName(), observation.getEntityTallTableName(), 
        mood.getId(), moods); 

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
