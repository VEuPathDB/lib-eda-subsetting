package org.veupathdb.service.eda.ss.model;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.ss.model.distribution.DistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;
import org.veupathdb.service.eda.ss.model.variable.FloatingPointVariable;
import org.veupathdb.service.eda.ss.model.variable.StringVariable;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableDataShape;
import org.veupathdb.service.eda.ss.model.filter.DateRangeFilter;
import org.veupathdb.service.eda.ss.model.filter.DateSetFilter;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.model.filter.NumberRangeFilter;
import org.veupathdb.service.eda.ss.model.filter.NumberSetFilter;
import org.veupathdb.service.eda.ss.model.filter.StringSetFilter;
import org.veupathdb.service.eda.ss.model.variable.VariableDisplayType;
import org.veupathdb.service.eda.ss.model.variable.VariableType;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

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

  public StringVariable city;
  public StringVariable roof;
  public FloatingPointVariable shoesize;
  public FloatingPointVariable weight;
  public FloatingPointVariable favNumber;
  public DateVariable birthDate;
  public DateVariable startDate;
  public StringVariable mood;
  public StringVariable haircolor;
  public FloatingPointVariable networth;
  public StringVariable earsize;
  public StringVariable waterSupply;
  
  public Filter obsWeightFilter;
  public Filter houseRoofFilter;
  public Filter obsFavNumberFilter; // categorical numeric
  public Filter obsBirthDateFilter;  // continuous date
  public Filter obsFavNewYearsFilter;  // categorical numeric
  public Filter obsMoodFilter; // string 
  public Filter houseObsWaterSupplyFilter; // string 
  
  public TestModel() {
    createTestEntities();
    Study.StudyOverview overview = new Study.StudyOverview("GEMS", "ds2324");
    study = new Study(overview, constructEntityTree(), createIdMap());
    constructVariables();
    createFilters();
  }
  
  private void createTestEntities() {
    household = new Entity("GEMS_House", "ds2324", "Household", "Households", "descrip", "Hshld", 1);
    householdObs = new Entity("GEMS_HouseObs", "ds2324", "Household Observation", "Household Observations", "descrip", "HshldObsvtn", 2);
    participant = new Entity("GEMS_Part", "ds2324", "Participant", "Participants", "descrip", "Prtcpnt", 3);
    observation = new Entity("GEMS_PartObs", "ds2324", "Observation", "Observations", "descrip", "PrtcpntObsrvtn", 4);
    sample = new Entity("GEMS_Sample", "ds2324", "Sample", "Samples", "descrip", "Smpl", 5);
    treatment = new Entity("GEMS_Treat", "ds2324", "Treatment", "Treatments", "descrip", "Trtmnt", 6);
  }
  
  private Map<String, Entity> createIdMap() {
    Map<String, Entity> idMap = new HashMap<>();
    idMap.put("GEMS_House", household);
    idMap.put("GEMS_HouseObs", householdObs);
    idMap.put("GEMS_Part", participant);
    idMap.put("GEMS_PartObs", observation);
    idMap.put("GEMS_Sample", sample);
    idMap.put("GEMS_Treat", treatment);
    return idMap;
   }

  /*
   * return a fresh entity tree.
   */
  public TreeNode<Entity> constructEntityTree() {
    TreeNode<Entity> householdNode = new TreeNode<>(household);

    TreeNode<Entity> houseObsNode = new TreeNode<>(householdObs);
    householdNode.addChildNode(houseObsNode);

    TreeNode<Entity> participantNode = new TreeNode<>(participant);
    householdNode.addChildNode(participantNode);

    TreeNode<Entity> observationNode = new TreeNode<>(observation);
    participantNode.addChildNode(observationNode);
    
    TreeNode<Entity> sampleNode = new TreeNode<>(sample);
    observationNode.addChildNode(sampleNode);
    
    TreeNode<Entity> treatmentNode = new TreeNode<>(treatment);
    observationNode.addChildNode(treatmentNode);
    
    return householdNode;
  }

  private static StringVariable getMockStringVar(String label, String id, Entity entity, int distinctValuesCount, boolean isMultiValued, List<String> vocab) {
    return new StringVariable(
        new Variable.Properties(label, id, entity, VariableDisplayType.DEFAULT, label, null, null, "Their " + label, Collections.emptyList()),
        new VariableWithValues.Properties(VariableType.STRING, VariableDataShape.CATEGORICAL, vocab, distinctValuesCount, false, false, false, isMultiValued)
    );
  }

  private static FloatingPointVariable getMockFloatVar(String label, String id, Entity entity, VariableDataShape shape, int distinctValuesCount, boolean isMultiValued) {
    return new FloatingPointVariable(
        new Variable.Properties(label, id, entity, VariableDisplayType.DEFAULT, label, null, null, "Their " + label, Collections.emptyList()),
        new VariableWithValues.Properties(VariableType.NUMBER, shape, null, distinctValuesCount, false, false, false, isMultiValued),
        new DistributionConfig<>(null, null, null, null, null, null),
        new FloatingPointVariable.Properties("", 1L)
    );
  }

  private static DateVariable getMockDateVar(String label, String id, Entity entity, VariableDataShape shape, int distinctValuesCount) {
    return new DateVariable(
        new Variable.Properties(label, id, entity, VariableDisplayType.DEFAULT, label, null, null, label, Collections.emptyList()),
        new VariableWithValues.Properties(VariableType.DATE, shape, null, distinctValuesCount, true, false, false, false),
        new DateVariable.Properties(shape, null, null, null, null, 1, "week", null)
    );
  }

  private void constructVariables() {

    /**************** String Variables ****************/

    city = getMockStringVar("city", "var_h1", household, 10, false, Arrays.asList("Boston", "Miami"));
    household.addVariable(city);

    roof = getMockStringVar("roof", "var_h2", household, 12, false, Arrays.asList("metal", "tile"));
    household.addVariable(roof);
    
    // multi-valued string var
    haircolor = getMockStringVar("haircolor", "var_p4", participant, 21, true, Arrays.asList("blond", "brown", "silver"));
    participant.addVariable(haircolor);
    
    mood = getMockStringVar("mood", "var_o5", observation, 96, false, Arrays.asList("happy", "jolly", "giddy"));
    observation.addVariable(mood);

    waterSupply = getMockStringVar("waterSupply", "var_ho1", householdObs, 66, false, Arrays.asList("piped", "well"));
    householdObs.addVariable(waterSupply);

    earsize = getMockStringVar("earsize", "var_p5", participant, 87, false, Arrays.asList("small", "medium", "large"));
    participant.addVariable(earsize);

    /**************** Float/Number Variables ****************/

    // multi-valued number var (what can i say... left and right feet are different!)
    shoesize = getMockFloatVar("shoesize", "var_p2", participant, VariableDataShape.CATEGORICAL, 47, true);
    participant.addVariable(shoesize);

    networth = getMockFloatVar("networth", "var_p1", participant, VariableDataShape.CONTINUOUS, 875, false);
    participant.addVariable(networth);

    weight = getMockFloatVar("weight", "var_o1", observation, VariableDataShape.CONTINUOUS, 65, false);
    observation.addVariable(weight);

    favNumber = getMockFloatVar("favNumber", "var_o2", observation, VariableDataShape.CATEGORICAL, 312, false);
    observation.addVariable(favNumber);

    /**************** Date Variables ****************/

    birthDate = getMockDateVar("visitDate", "var_o4", observation, VariableDataShape.CONTINUOUS, 13);
    observation.addVariable(birthDate);
    
    startDate = getMockDateVar("startDate", "var_o3", observation, VariableDataShape.CATEGORICAL, 74);
    observation.addVariable(startDate);

  }

  private void createFilters() {

    // create observation weight filter
    obsWeightFilter = new NumberRangeFilter(observation, weight, 10, 20);

    List<Number> favNums = Arrays.asList(new Number[]{5,7,9});
    obsFavNumberFilter = new NumberSetFilter(observation, favNumber, favNums);

    obsBirthDateFilter = new DateRangeFilter(observation, birthDate,
        LocalDateTime.of(2019, Month.MARCH, 21, 0, 0),
        LocalDateTime.of(2019, Month.MARCH, 28, 0, 0));

    List<LocalDateTime> dates = new ArrayList<>();
    dates.add(LocalDateTime.of(2019, Month.MARCH, 21, 0, 0));
    dates.add(LocalDateTime.of(2019, Month.MARCH, 28, 0, 0));
    dates.add(LocalDateTime.of(2019, Month.JUNE, 12, 0, 0));
    obsFavNewYearsFilter = new DateSetFilter(observation, startDate, dates);

    List<String> moods = Arrays.asList("happy", "jolly", "giddy");
    obsMoodFilter = new StringSetFilter(observation, mood, moods);

    obsWeightFilter = new NumberRangeFilter(observation, weight, 10, 20);

    // create household roof filter
    List<String> roofs = Arrays.asList("metal", "tile");
    houseRoofFilter = new StringSetFilter(household, roof, roofs);

    // create household observation filter
    List<String> waterSupplies = Arrays.asList("piped", "well");
    houseObsWaterSupplyFilter = new StringSetFilter(householdObs, waterSupply, waterSupplies);
  }
}
