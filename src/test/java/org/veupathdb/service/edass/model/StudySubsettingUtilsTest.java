package org.veupathdb.service.edass.model;

import java.util.Collections;
import static org.gusdb.fgputil.FormatUtil.NL;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.edass.Resources;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;

import org.gusdb.fgputil.functional.TreeNode;

import static org.veupathdb.service.edass.model.RdbmsColumnNames.*;

public class StudySubsettingUtilsTest {

  private static TestModel model;
  private static DataSource datasource;
  
  // filters using data from the test db
  private static Filter houseCityFilter;
  private static Filter obsWeightFilter;
  private static Filter partHairFilter;
  private static Filter obsFavNumberFilter; // categorical numeric
  private static Filter obsBirthDateFilter;  // continuous date
  private static Filter obsVisitDateFilter;  // categorical numeric
  private static Filter obsMoodFilter; // string 
  private static Filter houseObsWaterSupplyFilter; // string 

  @BeforeAll
  public static void setUp() {
    model = new TestModel();
    datasource = Resources.getApplicationDataSource();    
    Study study = Study.loadStudy(datasource, "DS12385");
    createFiltersForDb(study);
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
    assertTrue(entityIdsInFilters.contains(model.observation.getId()), "ID set does not contain observ.");
    assertTrue(entityIdsInFilters.contains(model.household.getId()), "ID set does not contain household.");
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
    
    if (!t1.getContents().getId().equals(t2.getContents().getId())) return false;
    
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
    String expectedWithClause = model.householdObs.getWithClauseName() + " as (" + NL +
        "  SELECT " + model.household.getPKColName() + ", " +  model.householdObs.getPKColName() + " FROM " + model.householdObs.getAncestorsTableName() + NL +
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
 
    List<String> selectColsList = new ArrayList<String>();
    for (String name : model.observation.getAncestorPkColNames()) selectColsList.add("a." + name);
    selectColsList.add("t." + model.observation.getPKColName());
    String selectCols = String.join(", ", selectColsList);

    //      SELECT a.household_id, a.participant_id, t.observation_id
  //  FROM Obs_tall t, Obs_ancestors a

    String obsBase = "  SELECT " + String.join(", ", selectCols) + NL +
        "  FROM " + model.observation.getTallTableName() + " t, " +
        model.observation.getAncestorsTableName() + " a" + NL +
        "  WHERE t." + model.observation.getPKColName() + " = a." + model.observation.getPKColName() + NL;
    
    String expectedWithClause = model.observation.getWithClauseName() +  " as (" + NL +
        obsBase + 
        "  AND " + VARIABLE_ID_COL_NAME + " = '" + model.weight.getId() + "'" + NL +
        "  AND " + NUMBER_VALUE_COL_NAME + " >= 10 AND " + NUMBER_VALUE_COL_NAME + " <= 20" + NL +
        "INTERSECT" + NL +
        obsBase + 
        "  AND " + VARIABLE_ID_COL_NAME + " = '" + model.favNewYears.getId() + "'" + NL +
        "  AND " + DATE_VALUE_COL_NAME + " IN ('2019-03-21T00:00', '2019-03-28T00:00', '2019-06-12T00:00')" + NL +
        "INTERSECT" + NL +
        obsBase + 
        "  AND " + VARIABLE_ID_COL_NAME + " = '" + model.birthDate.getId() + "'" + NL +
        "  AND " + DATE_VALUE_COL_NAME + " >= '2019-03-21T00:00' AND " + DATE_VALUE_COL_NAME + " <= '2019-03-28T00:00'" + NL +
        "INTERSECT" + NL +
        obsBase + 
        "  AND " + VARIABLE_ID_COL_NAME + " = '" + model.mood.getId() + "'" + NL +
        "  AND " + STRING_VALUE_COL_NAME + " IN ('happy', 'jolly', 'giddy')" + NL +
        "INTERSECT" + NL +
        obsBase + 
        "  AND " + VARIABLE_ID_COL_NAME + " = '" + model.favNumber.getId() + "'" + NL +
        "  AND " + NUMBER_VALUE_COL_NAME + " IN (5, 7, 9 )" + NL +
        ")";
    assertEquals(expectedWithClause, withClause);
  }

  @Test
  @DisplayName("Test creating a select clause for tabular report")
  void testGenerateTabularSelectClause() {
    
    String selectClause = StudySubsettingUtils.generateTabularSelectClause(model.observation, "t", "a");
    String expectedSelectClause = "SELECT a." + model.household.getPKColName() +
        ", a." + model.participant.getPKColName() +
        ", t." + model.observation.getPKColName() +
        ", " + STRING_VALUE_COL_NAME + ", " + NUMBER_VALUE_COL_NAME + ", " + DATE_VALUE_COL_NAME;
    assertEquals(expectedSelectClause, selectClause);
  }

  @Test
  @DisplayName("Test getting full ancestor PKs list")
  void testGetFullAncestorPKs() {
    Set<String> cols = new HashSet<String>(model.observation.getAncestorFullPkColNames());
    Set<String> expected = new HashSet<String>(Arrays.asList(new String[]{model.household.getFullPKColName(), model.participant.getFullPKColName()}));
    assertEquals(expected, cols);
  }
  
  @Test
  @DisplayName("Test populating ancestors")
  void testPopulateAncestors() {
    Entity e = model.study.getEntity(model.household.getId()).orElse(null);
    assertEquals(new ArrayList<Entity>(), e.getAncestorEntities());
    
    e = model.study.getEntity(model.participant.getId()).orElse(null);
    List<Entity> l = Arrays.asList(new Entity[]{model.household});
    assertEquals(l, e.getAncestorEntities());

    e = model.study.getEntity(model.householdObs.getId()).orElse(null);
    l = Arrays.asList(new Entity[]{model.household});
    assertEquals(l, e.getAncestorEntities());

    e = model.study.getEntity(model.observation.getId()).orElse(null);
    l = Arrays.asList(new Entity[]{model.household, model.participant});
    assertEquals(l, e.getAncestorEntities());
  
    e = model.study.getEntity(model.sample.getId()).orElse(null);
    l = Arrays.asList(new Entity[]{model.household, model.participant, model.observation});
    assertEquals(l, e.getAncestorEntities());
  
    e = model.study.getEntity(model.treatment.getId()).orElse(null);
    l = Arrays.asList(new Entity[]{model.household, model.participant, model.observation});
    assertEquals(l, e.getAncestorEntities());
  
  }
  
  @Test
  @DisplayName("Test creating a where clause for tabular report")
  void testGenerateTabularWhereClause() {
    
    List<String> vars = Arrays.asList(new String[]{model.birthDate.getId(), model.favNumber.getId()});
    String where = StudySubsettingUtils.generateTabularWhereClause(vars, model.observation.getPKColName(), "t", "a");
    String expected = "WHERE (" + NL +
        "  " + VARIABLE_ID_COL_NAME + " = '" + model.birthDate.getId() + "' OR" + NL +
        "  " + VARIABLE_ID_COL_NAME + " = '" + model.favNumber.getId() + "'" + NL +
        ")" + NL +
        "AND t." + model.observation.getPKColName() + " = a." + model.observation.getPKColName();

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

    List<String> from = Arrays.asList(new String[]{model.household.getWithClauseName(), model.householdObs.getWithClauseName(), model.observation.getWithClauseName()});
    String inClause = StudySubsettingUtils.generateInClause(prunedTree, outputEntity, "t", "AND");
    String expected = "AND t." + model.householdObs.getPKColName() + " IN (" + NL +
        "  SELECT " + model.householdObs.getFullPKColName() + NL +
        "  FROM " + String.join(", ", from) + NL +
        "  WHERE " + model.household.getFullPKColName() + " = " + model.householdObs.getWithClauseName() + "." + model.household.getPKColName() + NL +
        "  AND " + model.household.getFullPKColName() + " = " + model.observation.getWithClauseName() + "." + model.household.getPKColName() + NL +
        ")";

    assertEquals(expected, inClause);
  }

  @Test
  @DisplayName("Test getting full tabular sql")
  void testGetTabularSql() {
    
    List<Filter> filters = getSomeFilters();
    
    List<String> outputVariableNames = Arrays.asList(new String[]{model.networth.getId(), model.shoesize.getId()});

    TreeNode<Entity> prunedTree = StudySubsettingUtils.pruneTree(model.study.getEntityTree(), filters, model.participant);

    String sql = StudySubsettingUtils.generateTabularSql(outputVariableNames, model.participant, filters, prunedTree);
    assertNotEquals("", sql);
    //System.out.println("Tabular SQL:" + "\n" + sql);
  }

  @Test
  @DisplayName("Test getting full distribution sql")
  void testGetDistributionSql() {
    
    List<Filter> filters = getSomeFilters();
    
    TreeNode<Entity> prunedTree = StudySubsettingUtils.pruneTree(model.study.getEntityTree(), filters, model.participant);

    String sql = StudySubsettingUtils.generateDistributionSql(model.participant, model.shoesize, filters, prunedTree);
    assertNotEquals("", sql);
    //System.out.println("Distribution SQL:" + "\n" + sql);
  }
  
  @Test
  @DisplayName("Test getting count of entities sql")
  void testGetEntitiesCountSql() {
    
    List<Filter> filters = getSomeFilters();
    
    TreeNode<Entity> prunedTree = StudySubsettingUtils.pruneTree(model.study.getEntityTree(), filters, model.participant);

    String sql = StudySubsettingUtils.generateEntityCountSql(model.participant, filters, prunedTree);
    assertNotEquals("", sql);
    //System.out.println("Entity Count SQL:" + "\n" + sql);
  }

  @Test
  @DisplayName("Test getting count of entities that have a value for a variable sql")
  void testGetVariableCountSql() {
    
    List<Filter> filters = getSomeFilters();
    
    TreeNode<Entity> prunedTree = StudySubsettingUtils.pruneTree(model.study.getEntityTree(), filters, model.participant);

    String sql = StudySubsettingUtils.generateVariableCountSql(model.participant, model.networth, filters, prunedTree);
    assertNotEquals("", sql);
    //System.out.println("Variable Count SQL:" + "\n" + sql);
  }

  @Test
  @DisplayName("Test get entity count - no filters") 
  void testEntityCountNoFiltersFromDb() {
    
    Study study = Study.loadStudy(datasource, "DS12385");

    String entityId = "GEMS_Part";
    Entity entity = study.getEntity(entityId).orElseThrow();

    String varId = "var-17";
    Variable var = entity.getVariable(varId).orElseThrow();

    Integer count = StudySubsettingUtils.getEntityCount(datasource, study, entity,
        new ArrayList<Filter>());
    
    assertEquals(4, count);
  }

  @Test
  @DisplayName("Test get entity count - with filters") 
  void testEntityCountFromDb() {
    
    Study study = Study.loadStudy(datasource, "DS12385");

    String entityId = "GEMS_Part";
    Entity entity = study.getEntity(entityId).orElseThrow();

    String varId = "var-17";
    Variable var = entity.getVariable(varId).orElseThrow();

    List<Filter> filters = new ArrayList<Filter>();
    filters.add(partHairFilter);
    filters.add(houseObsWaterSupplyFilter);

    Integer count = StudySubsettingUtils.getEntityCount(datasource, study, entity,
        filters);
    
    assertEquals(2, count);
  }

  @Test
  @DisplayName("Test get variable count - no filters") 
  void testVariableCountNoFiltersFromDb() {
    
    Study study = Study.loadStudy(datasource, "DS12385");

    String entityId = "GEMS_Part";
    Entity entity = study.getEntity(entityId).orElseThrow();

    String varId = "var-17";
    Variable var = entity.getVariable(varId).orElseThrow();

    List<Filter> filters = Collections.emptyList();

    TreeNode<Entity> prunedEntityTree = StudySubsettingUtils.pruneTree(study.getEntityTree(), filters, entity);

    Integer count = StudySubsettingUtils.getVariableCount(datasource, prunedEntityTree, entity, var, filters);
    
    assertEquals(4, count);
  }

  @Test
  @DisplayName("Test get variable count - with filters") 
  void testVariableCountFromDb() {
    
    Study study = Study.loadStudy(datasource, "DS12385");

    String entityId = "GEMS_Part";
    Entity entity = study.getEntity(entityId).orElseThrow();

    String varId = "var-17";
    Variable var = entity.getVariable(varId).orElseThrow();

    List<Filter> filters = new ArrayList<>();
    filters.add(partHairFilter);
    filters.add(houseObsWaterSupplyFilter);

    TreeNode<Entity> prunedEntityTree = StudySubsettingUtils.pruneTree(study.getEntityTree(), filters, entity);

    Integer count = StudySubsettingUtils.getVariableCount(datasource, prunedEntityTree, entity, var, filters);
    
    assertEquals(2, count);
  }

  @Test
  @DisplayName("Test variable distribution - no filters") 
  void testVariableDistributionNoFilters() {
    
    Study study = Study.loadStudy(datasource, "DS12385");

    String entityId = "GEMS_Part";
    Entity entity = study.getEntity(entityId).orElseThrow();

    String varId = "var-17";
    Variable var = entity.getVariable(varId).orElseThrow();

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StudySubsettingUtils.produceVariableDistribution(datasource, study, entity,
        var, new ArrayList<Filter>(), outputStream);
    
    String outStr = outputStream.toString();
    String[] rows = {"count\tvalue", "blond\t2", "brown\t1", "silver\t1"};
    String expected = String.join(NL, rows) + NL;
    assertEquals(expected, outStr);
  }

  @Test
  @DisplayName("Test variable distribution - with filters") 
  void testVariableDistribution() {
    
    Study study = Study.loadStudy(datasource, "DS12385");

    String entityId = "GEMS_Part";
    Entity entity = study.getEntity(entityId).orElseThrow();

    String varId = "var-17";
    Variable var = entity.getVariable(varId).orElseThrow();
    
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(houseCityFilter);
    filters.add(houseObsWaterSupplyFilter);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StudySubsettingUtils.produceVariableDistribution(datasource, study, entity,
        var, filters, outputStream);
    
    String outStr = outputStream.toString();
    String[] rows = {"count\tvalue", "brown\t1", "silver\t1"};
    String expected = String.join(NL, rows) + NL;
    assertEquals(expected, outStr);
  }

  List<Filter> getSomeFilters() {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(model.obsWeightFilter);
    filters.add(model.obsFavNewYearsFilter);
    filters.add(model.obsBirthDateFilter);
    filters.add(model.obsMoodFilter);
    filters.add(model.obsFavNumberFilter);
    filters.add(model.houseRoofFilter);
    filters.add(model.houseObsWaterSupplyFilter);
    return filters;
  }
  
  // filters using the test db
  static void createFiltersForDb(Study study) {
    
    Entity household = study.getEntity("GEMS_House").orElseThrow();
    Entity householdObs = study.getEntity("GEMS_HouseObs").orElseThrow();
    Entity participant = study.getEntity("GEMS_Part").orElseThrow();
    Entity observation = study.getEntity("GEMS_PartObs").orElseThrow();
    
    Variable city = household.getVariable("var-18").orElseThrow();  
    Variable watersupply = householdObs.getVariable("var-19").orElseThrow();  
    Variable haircolor = participant.getVariable("var-17").orElseThrow();  
    Variable weight = observation.getVariable("var-12").orElseThrow();  
    Variable favNumber = observation.getVariable("var-13").orElseThrow();  
    Variable startDate = observation.getVariable("var-14").orElseThrow();  
    Variable visitDate = observation.getVariable("var-15").orElseThrow();  
    Variable mood = observation.getVariable("var-16").orElseThrow();  

    List<String> haircolors = Arrays.asList(new String[]{"blond", "green"});
    partHairFilter = new StringSetFilter(participant, haircolor.getId(), haircolors); 

    obsWeightFilter = new NumberRangeFilter(observation, weight.getId(), 10, 20);

    List<Number> favNums = Arrays.asList(new Number[]{5,7,9});
    obsFavNumberFilter = new NumberSetFilter(observation, favNumber.getId(), favNums); 

    obsBirthDateFilter = new DateRangeFilter(observation, startDate.getId(),
        LocalDateTime.of(2019, Month.MARCH, 21, 00, 00),
        LocalDateTime.of(2019, Month.MARCH, 28, 00, 00));

    List<LocalDateTime> dates = new ArrayList<LocalDateTime>();
    dates.add(LocalDateTime.of(2019, Month.MARCH, 21, 00, 00));
    dates.add(LocalDateTime.of(2019, Month.MARCH, 28, 00, 00));
    dates.add(LocalDateTime.of(2019, Month.JUNE, 12, 00, 00));
    obsVisitDateFilter = new DateSetFilter(observation, visitDate.getId(), dates);

    List<String> moods = Arrays.asList(new String[]{"happy", "jolly", "giddy"});
    obsMoodFilter = new StringSetFilter(observation, mood.getId(), moods); 

    obsWeightFilter = new NumberRangeFilter(observation, weight.getId(), 10, 20);

    List<String> cities = Arrays.asList(new String[]{"Boston"});
    houseCityFilter = new StringSetFilter(household, city.getId(), cities);

    List<String> waterSupplies = Arrays.asList(new String[]{"piped", "well"});
    houseObsWaterSupplyFilter = new StringSetFilter(householdObs, watersupply.getId(), waterSupplies);
  }

}
