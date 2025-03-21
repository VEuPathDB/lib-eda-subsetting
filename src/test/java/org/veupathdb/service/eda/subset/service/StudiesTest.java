package org.veupathdb.service.eda.subset.service;

import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.fgputil.distribution.DistributionResult;
import org.gusdb.fgputil.distribution.HistogramBin;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.db.VariableFactory;
import org.veupathdb.service.eda.subset.model.reducer.EmptyBinaryMetadataProvider;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.subset.test.MockFilters;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.db.StudyFactory;
import org.veupathdb.service.eda.subset.model.distribution.DistributionFactory;
import org.veupathdb.service.eda.subset.model.distribution.ValueSpec;
import org.veupathdb.service.eda.subset.model.filter.Filter;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;
import org.veupathdb.service.eda.subset.test.StubDb;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StudiesTest {

  private static DatabaseInstance _db;
  private static MockFilters _filtersForTesting;
  private static VariableFactory _variableFactory;
  private static BinaryFilesManager _binaryFilesManager;

  @BeforeAll
  public static void setUp() {
    _db = StubDb.getDatabaseInstance();
    _binaryFilesManager = Mockito.mock(BinaryFilesManager.class);
    Mockito.when(_binaryFilesManager.studyHasFiles(Mockito.anyString())).thenReturn(false);
    _variableFactory = new VariableFactory(_db.getDataSource(), StubDb.APP_DB_SCHEMA, new EmptyBinaryMetadataProvider(), studyId -> false);
    Study study = new StudyFactory(_db.getDataSource(), StubDb.APP_DB_SCHEMA, StubDb.USER_STUDIES_FLAG, _variableFactory, true).getStudyById("DS-2324");
    _filtersForTesting = new MockFilters(study);
  }


  @Test
  @DisplayName("Test variable distribution - no filters")
  void testVariableDistributionNoFilters() {
    Study study = new StudyFactory(_db.getDataSource(), StubDb.APP_DB_SCHEMA, StubDb.USER_STUDIES_FLAG, _variableFactory, true).getStudyById("DS-2324");

    String entityId = "GEMS_Part";
    Entity entity = study.getEntity(entityId).orElseThrow();

    String varId = "var_p4";
    VariableWithValues<?> var = (VariableWithValues<?>)entity.getVariable(varId).orElseThrow();

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
    Study study = new StudyFactory(_db.getDataSource(), StubDb.APP_DB_SCHEMA, StubDb.USER_STUDIES_FLAG, _variableFactory, true).getStudyById("DS-2324");

    String entityId = "GEMS_Part";
    Entity entity = study.getEntity(entityId).orElseThrow();

    String varId = "var_p4";
    VariableWithValues<?> var = (VariableWithValues<?>)entity.getVariable(varId).orElseThrow();

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

  private void testDistributionResponse(Study study, Entity entity, VariableWithValues<?> var,
      List<Filter> filters, int expectedVariableCount, Map<String, Integer> expectedDistribution) {

    DistributionResult result = DistributionFactory.processDistributionRequest(
        _db, StubDb.APP_DB_SCHEMA, study, entity, var, filters, ValueSpec.COUNT, Optional.empty());

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
