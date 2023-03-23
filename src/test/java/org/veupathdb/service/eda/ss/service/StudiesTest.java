package org.veupathdb.service.eda.ss.service;

import org.gusdb.fgputil.distribution.DistributionResult;
import org.gusdb.fgputil.distribution.HistogramBin;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.db.VariableFactory;
import org.veupathdb.service.eda.ss.model.reducer.EmptyBinaryMetadataProvider;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.ss.test.MockFilters;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.db.StudyFactory;
import org.veupathdb.service.eda.ss.model.distribution.DistributionFactory;
import org.veupathdb.service.eda.ss.model.distribution.ValueSpec;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;
import org.veupathdb.service.eda.ss.test.StubDb;

import javax.sql.DataSource;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.veupathdb.service.eda.ss.test.StubDb.*;

public class StudiesTest {

  private static DataSource _dataSource;
  private static MockFilters _filtersForTesting;
  private static VariableFactory _variableFactory;
  private static BinaryFilesManager _binaryFilesManager;

  @BeforeAll
  public static void setUp() {
    _dataSource = StubDb.getDataSource();
    _binaryFilesManager = Mockito.mock(BinaryFilesManager.class);
    Mockito.when(_binaryFilesManager.studyHasFiles(Mockito.anyString())).thenReturn(false);
    _variableFactory = new VariableFactory(_dataSource, APP_DB_SCHEMA, new EmptyBinaryMetadataProvider(), _binaryFilesManager);
    Study study = new StudyFactory(_dataSource, APP_DB_SCHEMA, USER_STUDIES_FLAG, _variableFactory).getStudyById("DS-2324");
    _filtersForTesting = new MockFilters(study);
  }


  @Test
  @DisplayName("Test variable distribution - no filters")
  void testVariableDistributionNoFilters() {
    Study study = new StudyFactory(_dataSource, APP_DB_SCHEMA, USER_STUDIES_FLAG, _variableFactory).getStudyById("DS-2324");

    String entityId = "GEMS_Part";
    Entity entity = study.getEntity(entityId).orElseThrow();

    String varId = "var_p4";
    VariableWithValues var = (VariableWithValues)entity.getVariable(varId).orElseThrow();

    List<Filter> filters = Collections.emptyList();

    int expectedVariableCount = 4;

    Map<String, Integer> expectedDistribution = new HashMap<>(){{
      put("blond", 2);
      put("brown", 1);
      put("silver", 1);
    }};

    testDistributionResponse(study, entity, var, filters, expectedVariableCount, expectedDistribution);
  }

  @Test
  @DisplayName("Test variable distribution - with filters")
  void testVariableDistribution() {
    Study study = new StudyFactory(_dataSource, APP_DB_SCHEMA, USER_STUDIES_FLAG, _variableFactory).getStudyById("DS-2324");

    String entityId = "GEMS_Part";
    Entity entity = study.getEntity(entityId).orElseThrow();

    String varId = "var_p4";
    VariableWithValues var = (VariableWithValues)entity.getVariable(varId).orElseThrow();

    List<Filter> filters = new ArrayList<>();
    filters.add(_filtersForTesting.houseCityFilter);
    filters.add(_filtersForTesting.houseObsWaterSupplyFilter);

    int expectedVariableCount = 2;

    Map<String, Integer> expectedDistribution = new HashMap<>(){{
      put("brown", 1);
      put("silver", 1);
    }};

    testDistributionResponse(study, entity, var, filters, expectedVariableCount, expectedDistribution);
  }

  private void testDistributionResponse(Study study, Entity entity, VariableWithValues var,
      List<Filter> filters, int expectedVariableCount, Map<String, Integer> expectedDistribution) {

    DistributionResult result = DistributionFactory.processDistributionRequest(
        _dataSource, APP_DB_SCHEMA, study, entity, var, filters, ValueSpec.COUNT, Optional.empty());

    // check variable count
    assertEquals(expectedVariableCount, result.getStatistics().getNumDistinctEntityRecords());

    List<HistogramBin> responseRows = result.getHistogramData();

    // check number of distribution rows
    assertEquals(expectedDistribution.size(), responseRows.size());

    for (Map.Entry<String,Integer> expectedRow : expectedDistribution.entrySet()) {
      // find row in list
      HistogramBin bin = responseRows.stream().filter(b -> b.getBinLabel().equals(expectedRow.getKey())).findFirst()
          .orElseThrow(() -> new RuntimeException("expected bin row '" + expectedRow.getKey() + "' not found in result"));
      int count = bin.getValue().intValue(); // will throw if not integer
      // check distribution size for key
      assertEquals(expectedRow.getValue(), count);
    }
  }

}
