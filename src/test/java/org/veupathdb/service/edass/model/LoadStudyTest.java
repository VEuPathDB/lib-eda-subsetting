package org.veupathdb.service.edass.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import javax.sql.DataSource;

import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.TreeNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.edass.Resources;

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
    assertEquals("GEMS-House", entity.getId());
    assertEquals("Households from the study area", entity.getDescription());
    assertEquals( "Household", entity.getName());
  }
  
  @Test
  @DisplayName("Test creating entity tree") 
  void testCreateEntityTree() {
    TreeNode<Entity> entityTree = EntityResultSetUtils.getStudyEntityTree(datasource, "DS12385");
    assertNotNull(entityTree);
  }
  


}
