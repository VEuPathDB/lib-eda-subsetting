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
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.ss.model.variable.binary.SimpleStudyFinder;
import org.veupathdb.service.eda.ss.testutil.IndiaICEMRStudy;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class DataFlowMapReduceTreeTest {
  private static final ExecutorService THREAD_POOL = Executors.newSingleThreadExecutor();
  private final Path binaryDirectory = Path.of(System.getProperty("user.dir"), "src/test/resources");
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
        TabularResponses.Type.TABULAR.getBinaryFormatter(), new TabularReportConfig(), outputStream,
        new BinaryValuesStreamer(new BinaryFilesManager(binaryDirectory), THREAD_POOL, THREAD_POOL));
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
        TabularResponses.Type.TABULAR.getBinaryFormatter(), new TabularReportConfig(), outputStream, new BinaryValuesStreamer(
                new BinaryFilesManager(binaryDirectory), THREAD_POOL, THREAD_POOL));
  }

  @Test
  public void testMultiFilterDifferentEntity() {
    MultiFilterSubFilter f1 = new MultiFilterSubFilter(indiaICEMRStudy.getHouseholdMosquitoRepellentCoils(), List.of("No"));
    MultiFilterSubFilter f2 = new MultiFilterSubFilter(indiaICEMRStudy.getHouseholdMosquitoRepellentMats(), List.of("No"));
    MultiFilter multiFilter = new MultiFilter("test",
        indiaICEMRStudy.getHouseholdEntity(),
        List.of(f1, f2),
        MultiFilter.MultiFilterOperation.UNION);
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    FilteredResultFactory.produceTabularSubsetFromFile(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getParticipantEntity(),
        List.of(indiaICEMRStudy.getTimeSinceLastMalaria()), List.of(multiFilter),
        TabularResponses.Type.TABULAR.getBinaryFormatter(), new TabularReportConfig(), outputStream, new BinaryValuesStreamer(
                new BinaryFilesManager(binaryDirectory), THREAD_POOL, THREAD_POOL));
  }

  @Test
  public void testStringSetFilter() {
    StringSetFilter stringSetFilter = new StringSetFilter("test", indiaICEMRStudy.getParticipantEntity(),
        indiaICEMRStudy.getSymptoms(),
        List.of("Aches and pains"));
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    FilteredResultFactory.produceTabularSubsetFromFile(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getParticipantEntity(),
        List.of(indiaICEMRStudy.getTimeSinceLastMalaria()), List.of(stringSetFilter),
        TabularResponses.Type.TABULAR.getBinaryFormatter(), new TabularReportConfig(), outputStream, new BinaryValuesStreamer(
                new BinaryFilesManager(binaryDirectory), THREAD_POOL, THREAD_POOL));
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
        TabularResponses.Type.TABULAR.getBinaryFormatter(), new TabularReportConfig(), outputStream, new BinaryValuesStreamer(
                new BinaryFilesManager(binaryDirectory), THREAD_POOL, THREAD_POOL));
    MatcherAssert.assertThat(Arrays.stream(outputStream.toString().split("\n"))
        .collect(Collectors.toList()), Matchers.hasSize(63));
  }

  @Test
  public void noFilters() {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    FilteredResultFactory.produceTabularSubsetFromFile(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getParticipantEntity(),
        List.of(indiaICEMRStudy.getTimeSinceLastMalaria()), List.of(),
        TabularResponses.Type.TABULAR.getBinaryFormatter(), new TabularReportConfig(), outputStream,
            new BinaryValuesStreamer(new BinaryFilesManager(binaryDirectory), THREAD_POOL, THREAD_POOL));
    MatcherAssert.assertThat(Arrays.stream(outputStream.toString().split("\n"))
        .collect(Collectors.toList()), Matchers.hasSize(498));
  }

  @Test
  public void testOutputDateVariables() {
    TabularReportConfig config = new TabularReportConfig();
    config.setTrimTimeFromDateVars(true);
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    FilteredResultFactory.produceTabularSubsetFromFile(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getParticipantEntity(),
        List.of(indiaICEMRStudy.getObservationDate()), List.of(),
        TabularResponses.Type.TABULAR.getBinaryFormatter(), config, outputStream,
        new BinaryValuesStreamer(new BinaryFilesManager(binaryDirectory), THREAD_POOL, THREAD_POOL));
    MatcherAssert.assertThat(Arrays.stream(outputStream.toString().split("\n"))
        .collect(Collectors.toList()), Matchers.hasSize(498));
    System.out.println(outputStream);
  }
}
