package org.veupathdb.service.eda.ss.model.db;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.TreeNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.test.MockModel;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableDataShape;
import org.veupathdb.service.eda.ss.model.variable.VariableDisplayType;
import org.veupathdb.service.eda.ss.model.variable.VariableType;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;
import org.veupathdb.service.eda.ss.test.StubDb;

import static org.junit.jupiter.api.Assertions.*;
import static org.veupathdb.service.eda.ss.test.StubDb.*;

public class LoadStudyTest {
  
  private static DataSource datasource;

  public final static String STUDY_ID = "DS-2324";
  
  @BeforeAll
  public static void setUp() {
    datasource = StubDb.getDataSource();
  }
  
  @Test
  @DisplayName("Test reading of entity table") 
  void testReadEntityTable() {
    
    String sql = EntityFactory.generateEntityTreeSql(STUDY_ID, APP_DB_SCHEMA);

    // get the alphabetically first entity
    Entity entity = new SQLRunner(datasource, sql).executeQuery(rs -> {
      rs.next();
      return EntityFactory.createEntityFromResultSet(rs);
    });

    assertEquals("GEMS_House", entity.getId());
    assertEquals("Households in the study", entity.getDescription());
    assertEquals("Household", entity.getDisplayName());
    assertEquals("Households", entity.getDisplayNamePlural());
    assertEquals("Hshld", entity.getAbbreviation());
  }
  
  @Test
  @DisplayName("Test creating entity tree") 
  void testCreateEntityTree() {
    TreeNode<Entity> entityTree = new EntityFactory(datasource, APP_DB_SCHEMA).getStudyEntityTree(STUDY_ID);
    
    List<String> entityIds = entityTree.flatten().stream().map(Entity::getId).collect(Collectors.toList());

    // this is an imperfect, but good enough, test.  it is possible a wrong tree would flatten like this, but very unlikely.
    List<String> expected = Arrays.asList("GEMS_House", "GEMS_HouseObs", "GEMS_Part", "GEMS_PartObs", "GEMS_Sample", "GEMS_Treat");

    assertEquals(expected, entityIds);
  }
  
  @Test
  @DisplayName("Test reading of variable table") 
  void testReadVariableTable() {

    TreeNode<Entity> entityTree = new EntityFactory(datasource, APP_DB_SCHEMA).getStudyEntityTree(STUDY_ID);
    
    Map<String, Entity> entityIdMap = entityTree.flatten().stream().collect(Collectors.toMap(Entity::getId, e -> e));

    Entity entity = entityIdMap.get("GEMS_Part");
    
    String sql = VariableFactory.generateStudyVariablesListSql(new MockModel().participant, APP_DB_SCHEMA);
    
    Variable var = new SQLRunner(datasource, sql).executeQuery(rs -> {
      rs.next();
      return VariableFactory.createVariableFromResultSet(rs, entity);
    });

   // --(stable_id, ontology_term_id, parent_stable_id, provider_label, display_name, term_type, has_value, data_type, has_multiple_values_per_entity, data_shape, unit, unit_ontology_term_id, precision)
   // insert into Attribute_ds2324_Prtcpnt values ('var_10', 300, null, '_networth', 'Net worth', null, 1, 'number', 0, 'continuous', 'dollars', null, 2);


    //insert into variable values ('var_p1', 300, 'GEMS-Part', null, '_networth', 'Net worth', 1, 1, 'dollars', null);

    assertEquals("var_p1", var.getId());
    assertEquals("Net worth", var.getDisplayName());
    assertEquals("GEMS_Part", var.getEntityId());
    assertNull(var.getParentId());
    assertEquals("_networth", var.getProviderLabel());
    assertEquals(VariableDisplayType.DEFAULT, var.getDisplayType());
    assertTrue(var.hasValues());
    assertEquals(VariableType.NUMBER, ((VariableWithValues)var).getType());
    assertEquals(VariableDataShape.CONTINUOUS, ((VariableWithValues)var).getDataShape());
    List<String> hidden = var.getHideFrom();
    assertEquals(hidden.get(0), "downloads");
    assertEquals(hidden.get(1), "variableTree");
  }
  
  @Test
  @DisplayName("Test reading all participant variables")
  void testReadAllVariables() {

    TreeNode<Entity> entityTree = new EntityFactory(datasource, APP_DB_SCHEMA).getStudyEntityTree(STUDY_ID);
    
    Map<String, Entity> entityIdMap = entityTree.flatten().stream().collect(Collectors.toMap(Entity::getId, e -> e));

    Entity entity = entityIdMap.get("GEMS_Part");

    List<Variable> variables = new VariableFactory(datasource, APP_DB_SCHEMA).loadVariables(entity);
    
    assertEquals(5, variables.size());
  }
  
  @Test
  @DisplayName("Load study test") 
  void testLoadStudy() {
    Study study = new StudyFactory(datasource, APP_DB_SCHEMA, USER_STUDIES_FLAG).getStudyById(STUDY_ID);
    assertNotNull(study);
  }
}
