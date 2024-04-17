package org.veupathdb.service.eda.subset.model.db;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.reducer.BinaryMetadataProvider;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.subset.test.MockFilters;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.test.MockModel;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;
import org.veupathdb.service.eda.subset.test.StubDb;

public class EntityResultSetUtilsTest {

  private static MockModel _model;
  private BinaryMetadataProvider _binaryMetadataProvider;
  private BinaryFilesManager _binaryFilesManager;

  private Study study;

  @BeforeAll
  public static void setUp() {
    _model = new MockModel();
  }

  @BeforeEach
  public void beforeEach() {
    _binaryFilesManager = Mockito.mock(BinaryFilesManager.class);
    Mockito.when(_binaryFilesManager.studyHasFiles(Mockito.anyString())).thenReturn(false);
    _binaryMetadataProvider = Mockito.mock(BinaryMetadataProvider.class);
    Mockito.when(_binaryMetadataProvider.getBinaryProperties(Mockito.anyString(), Mockito.any(Entity.class), Mockito.anyString()))
            .thenReturn(Optional.empty());
    Study study = new StudyFactory(StubDb.getDataSource(), StubDb.APP_DB_SCHEMA, StubDb.USER_STUDIES_FLAG,
        new VariableFactory(StubDb.getDataSource(), StubDb.APP_DB_SCHEMA, _binaryMetadataProvider, studyId -> false), true).getStudyById(LoadStudyTest.STUDY_ID);
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
