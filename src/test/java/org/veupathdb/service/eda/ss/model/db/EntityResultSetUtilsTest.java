package org.veupathdb.service.eda.ss.model.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.veupathdb.service.eda.ss.test.StubDb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.test.MockFilters;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.test.MockModel;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;
import org.veupathdb.service.eda.ss.test.StubDb;

public class EntityResultSetUtilsTest {

  private static MockModel _model;

  @BeforeAll
  public static void setUp() {
    _model = new MockModel();
    Study study = new StudyFactory(StubDb.getDataSource(), APP_DB_SCHEMA, USER_STUDIES_FLAG).getStudyById(LoadStudyTest.STUDY_ID);
    new MockFilters(study);
  }
  
  @Test
  @DisplayName("Test getting set of entity IDs from set of filters ")
  void testPutMultiValuesAsJsonIntoWideRow() {
    
    List<String> hairColors = new ArrayList<String>();
    hairColors.add("red");
    hairColors.add("blue");
    
    List<String> shoeSizes = new ArrayList<String>();
    shoeSizes.add("9.5");
    shoeSizes.add("19");
    
    Map<String, List<String>> multiValues = new HashMap<String, List<String>>();
    multiValues.put(_model.haircolor.getId(), hairColors);
    multiValues.put(_model.shoesize.getId(), shoeSizes);
    
    Map<String, String> wideRow = new HashMap<String, String>();
    wideRow.put(_model.mood.getId(), "happy");
    
    Map<String, VariableWithValues> variablesMap = new HashMap<String, VariableWithValues>();
    variablesMap.put(_model.mood.getId(), _model.mood);
    variablesMap.put(_model.haircolor.getId(), _model.haircolor);
    variablesMap.put(_model.shoesize.getId(), _model.shoesize);
       
    TallRowConversionUtils.putMultiValuesAsJsonIntoWideRow(multiValues, wideRow, variablesMap);
    
    assertEquals("[\"red\",\"blue\"]", wideRow.get(_model.haircolor.getId()));
    assertEquals("[9.5,19]", wideRow.get(_model.shoesize.getId()));
   }

}
