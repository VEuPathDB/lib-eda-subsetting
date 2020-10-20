package org.veupathdb.service.edass.model;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
  public Variable haircolor;
  public Variable networth;
  public Variable waterSupply;
  
  public Filter obsWeightFilter;
  public Filter houseRoofFilter;
  public Filter obsFavNumberFilter; // categorical numeric
  public Filter obsBirthDateFilter;  // continuous date
  public Filter obsFavNewYearsFilter;  // categorical numeric
  public Filter obsMoodFilter; // string 
  public Filter houseObsWaterSupplyFilter; // string 
  
  public TestModel() {
    createTestEntities();
    study = new Study("555555", constructEntityTree(), constructVariables()); 
    createFilters();
  }
  
  private void createTestEntities() {
    household = new Entity("Household", "entity-1", "descrip", "Hshld_tall", "Hshld_ancestors",
        "household_id");
    householdObs = new Entity("HouseholdObs", "entity-4", "descrip", "HouseObs_tall", "HouseObs_ancestors",
        "household_obs_id");
    participant = new Entity("Participant", "entity-2", "descrip", "Part_tall", "Part_ancestors",
        "participant_id");
    observation = new Entity("Observation", "entity-3", "descrip", "Obs_tall", "Obs_ancestors",
        "observation_id");
    sample = new Entity("Sample", "entity-5", "descrip", "Sample_tall", "Sample_ancestors",
        "sample_id");
    treatment = new Entity("Treatment", "entity-6", "descrip", "Treatment_tall", "Treatment_ancestors",
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
  
  private List<Variable> constructVariables() {
    List<Variable> vars = new ArrayList<Variable>();

    roof = new Variable("roof", "var-10", household, VariableType.STRING, Resolution.CATEGORICAL);
    vars.add(roof);
    
    shoesize = new Variable("shoesize", "var-11", participant, VariableType.NUMBER, Resolution.CATEGORICAL);    
    vars.add(shoesize);

    haircolor = new Variable("haircolor", "var-17", participant, VariableType.STRING, Resolution.CATEGORICAL);    
    vars.add(shoesize);

    networth = new Variable("networth", "var-18", participant, VariableType.NUMBER, Resolution.CONTINUOUS);    
    vars.add(shoesize);

    weight = new Variable("weight", "var-12", observation, VariableType.NUMBER, Resolution.CONTINUOUS);    
    vars.add(weight);
    
    favNumber = new Variable("favNumber", "var-13", observation, VariableType.NUMBER, Resolution.CATEGORICAL);    
    vars.add(weight);
    
    birthDate  = new Variable("birthDate", "var-14", observation, VariableType.DATE, Resolution.CONTINUOUS);    
    vars.add(birthDate);
    
    favNewYears = new Variable("favNewYears", "var-15", observation, VariableType.DATE, Resolution.CATEGORICAL);    
    vars.add(favNewYears);
    
    mood  = new Variable("mood", "var-16", observation, VariableType.STRING, Resolution.CATEGORICAL);    
    vars.add(mood);

    waterSupply  = new Variable("waterSupply", "var-19", householdObs, VariableType.STRING, Resolution.CATEGORICAL);    
    vars.add(waterSupply);

    return vars;
  }
  
  private void createFilters() {
    
    // create observation weight filter
    obsWeightFilter = new NumberRangeFilter(observation, weight.getId(), 10, 20);

    List<Number> favNums = Arrays.asList(new Number[]{5,7,9});
    obsFavNumberFilter = new NumberSetFilter(observation, favNumber.getId(), favNums); 

    obsBirthDateFilter = new DateRangeFilter(observation, birthDate.getId(),
        LocalDateTime.of(2019, Month.MARCH, 21, 00, 00),
        LocalDateTime.of(2019, Month.MARCH, 28, 00, 00));

    List<LocalDateTime> dates = new ArrayList<LocalDateTime>();
    dates.add(LocalDateTime.of(2019, Month.MARCH, 21, 00, 00));
    dates.add(LocalDateTime.of(2019, Month.MARCH, 28, 00, 00));
    dates.add(LocalDateTime.of(2019, Month.JUNE, 12, 00, 00));
    obsFavNewYearsFilter = new DateSetFilter(observation, favNewYears.getId(), dates);

    List<String> moods = Arrays.asList(new String[]{"happy", "jolly", "giddy"});
    obsMoodFilter = new StringSetFilter(observation, mood.getId(), moods); 

    obsWeightFilter = new NumberRangeFilter(observation, weight.getId(), 10, 20);

    // create household roof filter
    List<String> roofs = Arrays.asList(new String[]{"metal", "tile"});
    houseRoofFilter = new StringSetFilter(household, roof.getId(), roofs);

    // create household observation filter
    List<String> waterSupplies = Arrays.asList(new String[]{"piped", "well"});
    houseObsWaterSupplyFilter = new StringSetFilter(householdObs, waterSupply.getId(), waterSupplies);
  }
}
