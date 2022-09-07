package org.veupathdb.service.eda.ss.model.reducer;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.db.FilteredResultFactory;
import org.veupathdb.service.eda.ss.model.filter.MultiFilter;
import org.veupathdb.service.eda.ss.model.filter.MultiFilterSubFilter;
import org.veupathdb.service.eda.ss.model.filter.NumberRangeFilter;
import org.veupathdb.service.eda.ss.model.filter.StringSetFilter;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.tabular.TabularResponses;
import org.veupathdb.service.eda.ss.testutil.IndiaICEMRStudy;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class DataFlowMapReduceTreeTest {
  private Path binaryDirectory = Path.of(System.getProperty("user.dir"), "src/test/resources");
  private final IndiaICEMRStudy indiaICEMRStudy = new IndiaICEMRStudy();

  @Test
  public void testThreeEntityFilter() {
    NumberRangeFilter<Long> sampleRangeFilter = new NumberRangeFilter<>(
        "test",
        indiaICEMRStudy.getSampleEntity(),
        indiaICEMRStudy.getPlasmoFalcGametocytes(),
        0L,
        5L
    );
    NumberRangeFilter<Long> timeSinceLastMalariaFilter = new NumberRangeFilter<>(
        "test",
        indiaICEMRStudy.getParticipantEntity(),
        indiaICEMRStudy.getTimeSinceLastMalaria(),
        60L,
        180L
    );
    NumberRangeFilter<Long> personsInHouseholdFilter = new NumberRangeFilter<>(
        "test",
        indiaICEMRStudy.getHouseholdEntity(),
        indiaICEMRStudy.getPersonsInHousehold(),
        0L,
        10L
    );
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    FilteredResultFactory.produceTabularSubsetFromFile(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getHouseholdEntity(),
        List.of(indiaICEMRStudy.getPersonsInHousehold()), List.of(sampleRangeFilter, timeSinceLastMalariaFilter, personsInHouseholdFilter),
        TabularResponses.Type.TABULAR.getFormatter(), new TabularReportConfig(), outputStream, binaryDirectory);
    MatcherAssert.assertThat(Arrays.stream(outputStream.toString().split("\n"))
        .collect(Collectors.toList()), Matchers.hasSize(1));
  }

  @Test
  public void testMultiFilter() {
    MultiFilterSubFilter f1 = new MultiFilterSubFilter(indiaICEMRStudy.getHouseholdMosquitoRepellentCoils(), List.of("No"));
    MultiFilter multiFilter = new MultiFilter("test",
        indiaICEMRStudy.getHouseholdEntity(),
        List.of(f1),
        MultiFilter.MultiFilterOperation.INTERSECT);
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    FilteredResultFactory.produceTabularSubsetFromFile(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getParticipantEntity(),
        List.of(indiaICEMRStudy.getTimeSinceLastMalaria()), List.of(multiFilter),
        TabularResponses.Type.TABULAR.getFormatter(), new TabularReportConfig(), outputStream, binaryDirectory);
  }

  @Test
  public void testMultiFilterDifferentEntity() {
    /**
     * {
     *   "reportConfig": {
     *     "dataSource": "{{data_source}}"
     *   },
     *   "filters": [
     *     {
     *       "entityId": "PCO_0000024",
     *       "variableId": "EUPATH_0021242",
     *       "type": "multiFilter",
     *       "operation": "intersect",
     *       "subFilters": [
     *         {
     *           "entityId": "PCO_0000024",
     *           "variableId": "EUPATH_0021243",
     *           "type": "stringSet",
     *           "stringSet": [
     *             "No"
     *           ]
     *         },
     *         {
     *           "entityId": "PCO_0000024",
     *           "variableId": "EUPATH_0021246",
     *           "type": "stringSet",
     *           "stringSet": [
     *             "No"
     *           ]
     *         }
     *       ]
     *     }
     *   ],
     *   "outputVariableIds": [
     *     "EUPATH_0000427"
     *   ]
     * }
     */
    MultiFilterSubFilter f1 = new MultiFilterSubFilter(indiaICEMRStudy.getHouseholdMosquitoRepellentCoils(), List.of("No"));
    MultiFilterSubFilter f2 = new MultiFilterSubFilter(indiaICEMRStudy.getHouseholdMosquitoRepellentMats(), List.of("No"));
    MultiFilter multiFilter = new MultiFilter("test",
        indiaICEMRStudy.getHouseholdEntity(),
        List.of(f1, f2),
        MultiFilter.MultiFilterOperation.UNION);
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    FilteredResultFactory.produceTabularSubsetFromFile(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getParticipantEntity(),
        List.of(indiaICEMRStudy.getTimeSinceLastMalaria()), List.of(multiFilter),
        TabularResponses.Type.TABULAR.getFormatter(), new TabularReportConfig(), outputStream, binaryDirectory);
    System.out.println(outputStream);
  }

  @Test
  public void testStringSetFilter() {
    StringSetFilter stringSetFilter = new StringSetFilter("test", indiaICEMRStudy.getParticipantEntity(),
        indiaICEMRStudy.getSymptoms(),
        List.of("Aches and pains"));
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    FilteredResultFactory.produceTabularSubsetFromFile(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getParticipantEntity(),
        List.of(indiaICEMRStudy.getTimeSinceLastMalaria()), List.of(stringSetFilter),
        TabularResponses.Type.TABULAR.getFormatter(), new TabularReportConfig(), outputStream, binaryDirectory);
    System.out.println(outputStream);
  }

  public void test500() {
    /**
     * {
     *   "reportConfig": {
     *     "dataSource": "{{data_source}}"
     *   },
     *   "filters": [
     *     {
     *       "entityId": "PCO_0000024",
     *       "variableId": "EUPATH_0021242",
     *       "type": "multiFilter",
     *       "operation": "union",
     *       "subFilters": [
     *         {
     *           "entityId": "PCO_0000024",
     *           "variableId": "EUPATH_0021243",
     *           "type": "stringSet",
     *           "stringSet": [
     *             "No"
     *           ]
     *         },
     *         {
     *           "entityId": "PCO_0000024",
     *           "variableId": "EUPATH_0021246",
     *           "type": "stringSet",
     *           "stringSet": [
     *             "No"
     *           ]
     *         }
     *       ]
     *     }
     *   ],
     *   "outputVariableIds": [
     *     "EUPATH_0021246"
     *   ]
     * }
     */
  }

  @Test
  public void testTwoEntityFilter() {
    NumberRangeFilter<Long> rangeFilter = new NumberRangeFilter<>(
        "test",
        indiaICEMRStudy.getParticipantEntity(),
        indiaICEMRStudy.getTimeSinceLastMalaria(),
        0L,
        180L
    );
    NumberRangeFilter<Long> rootRangeFilter = new NumberRangeFilter<>(
        "test",
        indiaICEMRStudy.getHouseholdEntity(),
        indiaICEMRStudy.getPersonsInHousehold(),
        0L,
        10L
    );
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    FilteredResultFactory.produceTabularSubsetFromFile(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getParticipantEntity(),
        List.of(indiaICEMRStudy.getTimeSinceLastMalaria()), List.of(rangeFilter, rootRangeFilter),
        TabularResponses.Type.TABULAR.getFormatter(), new TabularReportConfig(), outputStream, binaryDirectory);
    MatcherAssert.assertThat(Arrays.stream(outputStream.toString().split("\n"))
        .collect(Collectors.toList()), Matchers.hasSize(63));
  }

  @Test
  public void noFilters() {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    FilteredResultFactory.produceTabularSubsetFromFile(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getParticipantEntity(),
        List.of(indiaICEMRStudy.getTimeSinceLastMalaria()), List.of(),
        TabularResponses.Type.TABULAR.getFormatter(), new TabularReportConfig(), outputStream, binaryDirectory);
    MatcherAssert.assertThat(Arrays.stream(outputStream.toString().split("\n"))
        .collect(Collectors.toList()), Matchers.hasSize(498));
  }
}
