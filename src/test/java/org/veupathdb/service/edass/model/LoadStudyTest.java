package org.veupathdb.service.edass.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
import org.veupathdb.service.edass.Resources;
import org.veupathdb.service.edass.model.Variable.VariableType;

public class LoadStudyTest {
  
  private static DataSource datasource;
  
  @BeforeAll
  public static void setUp() {
    datasource = Resources.getApplicationDataSource();    
  }
  
  @Test
  @DisplayName("Test reading of entity table") 
  void testReadEntityTable() {
    
    String sql = EntityResultSetUtils.generateEntityTreeSql("DS12385");
    
    Entity entity = new SQLRunner(datasource, sql).executeQuery(rs -> {
      rs.next();
      Entity e = EntityResultSetUtils.createEntityFromResultSet(rs);
      return e;
    });
    assertEquals("GEMS_House", entity.getId());
    assertEquals("Households from the study area", entity.getDescription());
    assertEquals( "Household", entity.getDisplayName());
  }
  
  @Test
  @DisplayName("Test creating entity tree") 
  void testCreateEntityTree() {
    TreeNode<Entity> entityTree = EntityResultSetUtils.getStudyEntityTree(datasource, "DS12385");
    
    List<String> entityIds = entityTree.flatten().stream().map(e -> e.getId()).collect(Collectors.toList()); 

    // this is an imperfect, but good enough, test.  it is possible a wrong tree would flatten like this, but very unlikely.
    List<String> expected = Arrays.asList("GEMS_House", "GEMS_HouseObs", "GEMS_Part", "GEMS_PartObs", "GEMS_Sample", "GEMS_Treat");

    assertEquals(expected, entityIds);
  }
  
  @Test
  @DisplayName("Test reading of variable table") 
  void testReadVariableTable() {
    
    TreeNode<Entity> entityTree = EntityResultSetUtils.getStudyEntityTree(datasource, "DS12385");
    
    Map<String, Entity> entityIdMap = entityTree.flatten().stream().collect(Collectors.toMap(e -> e.getId(), e -> e)); 
    
    String sql = VariableResultSetUtils.generateStudyVariablesListSql("DS12385");
    
    Variable var = new SQLRunner(datasource, sql).executeQuery(rs -> {
      rs.next();
      Variable v = VariableResultSetUtils.createVariableFromResultSet(rs, entityIdMap);
      return v;
    });
    //insert into variable values ('var-10', 300, 'GEMS-Part', null, '_networth', 'Net worth', 1, 1, 'dollars', null);

    assertEquals("var-10", var.getId());
    assertEquals("Net worth", var.getDisplayName());
    assertEquals("GEMS_Part", var.getEntityId());
    assertEquals(Variable.IsContinuous.TRUE, var.getIsContinuous());
    assertEquals(null, var.getParentId());
    assertEquals(2, var.getPrecision());
    assertEquals("_networth", var.getProviderLabel());
    assertEquals(VariableType.NUMBER, var.getType());
    assertEquals("dollars", var.getUnits());

  }
  
  @Test
  @DisplayName("Test reading all variables") 
  void testReadAllVariables() {
    
    TreeNode<Entity> entityTree = EntityResultSetUtils.getStudyEntityTree(datasource, "DS12385");
    
    Map<String, Entity> entityIdMap = entityTree.flatten().stream().collect(Collectors.toMap(e -> e.getId(), e -> e)); 
    
    List<Variable> variables = VariableResultSetUtils.getStudyVariables(datasource, "DS12385", entityIdMap);
    
    assertEquals(11, variables.size());
  }
  
  @Test
  @DisplayName("Load study test") 
  void testLoadStudy() {
    
    Study study = Study.loadStudy(datasource, "DS12385");
    assertNotNull(study);
  }
  

  


}
