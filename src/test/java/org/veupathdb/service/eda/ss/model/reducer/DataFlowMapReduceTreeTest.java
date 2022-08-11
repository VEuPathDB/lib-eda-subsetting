package org.veupathdb.service.eda.ss.model.reducer;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.db.FilteredResultFactory;
import org.veupathdb.service.eda.ss.model.filter.NumberRangeFilter;
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
}
