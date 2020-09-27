package org.veupathdb.service.edass.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class StudySubsettingUtilsTest {
  
  private Study testStudy;
  
  @BeforeAll
  public void setUp() {
      testStudy = createTestStudy();
  }

  Study createTestStudy() {
    return null;
  }
  /*
  static String getSqlJoinString(Entity parentEntity, Entity childEntity) {
    return parentEntity.getEntityName() + "." + parentEntity.getEntityPrimaryKeyColumnName() + " = " +
        childEntity.getEntityName() + "." + childEntity.getEntityPrimaryKeyColumnName();
  }
   */
  @Test
  void test1() {
    Entity parent = null;
    Entity child = null;
    assertEquals("  ,,,   ", StudySubsettingUtils.getSqlJoinString(parent, child));
  }
}
