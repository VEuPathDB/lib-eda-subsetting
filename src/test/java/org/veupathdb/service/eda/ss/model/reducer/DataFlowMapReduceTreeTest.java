package org.veupathdb.service.eda.ss.model.reducer;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.filter.NumberRangeFilter;
import org.veupathdb.service.eda.ss.model.reducer.ancestor.EntityIdIndexIteratorConverter;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.ss.testutil.IndiaICEMRStudy;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class DataFlowMapReduceTreeTest {
  private Path binaryDirectory = Path.of(System.getProperty("user.dir"), "src/test/resources");
  private BinaryFilesManager binaryFilesManager = new BinaryFilesManager(binaryDirectory);
  private EntityIdIndexIteratorConverter entityIdIndexIteratorConverter = new EntityIdIndexIteratorConverter(binaryFilesManager);
  private BinaryValuesStreamer binaryValuesStreamer = new BinaryValuesStreamer(binaryDirectory);
  private final IndiaICEMRStudy indiaICEMRStudy = new IndiaICEMRStudy();

  @Test
  public void testThreeEntityFilter() throws IOException {
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
    SubsettingJoinNode sampleNode = new SubsettingJoinNode(
        List.of(binaryValuesStreamer.streamFilteredValues(sampleRangeFilter, indiaICEMRStudy.getStudy())),
        Collections.emptyList(),
        indiaICEMRStudy.getSampleEntity(),
        indiaICEMRStudy.getStudy(),
        entityIdIndexIteratorConverter
    );
    SubsettingJoinNode participantNode = new SubsettingJoinNode(
        List.of(binaryValuesStreamer.streamFilteredValues(timeSinceLastMalariaFilter, indiaICEMRStudy.getStudy())),
        List.of(sampleNode),
        indiaICEMRStudy.getParticipantEntity(),
        indiaICEMRStudy.getStudy(),
        entityIdIndexIteratorConverter
    );
    Iterator<VariableValueIdPair<?>> values = binaryValuesStreamer.streamIdValuePairs(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getPersonsInHousehold());
    List<VariableValueIdPair<List<Long>>> ancestorStream = List.of();
    SubsettingJoinNode root = new SubsettingJoinNode(
        List.of(binaryValuesStreamer.streamFilteredValues(personsInHouseholdFilter, indiaICEMRStudy.getStudy())),
        List.of(participantNode),
        indiaICEMRStudy.getHouseholdEntity(),
        indiaICEMRStudy.getStudy(),
        entityIdIndexIteratorConverter);
    DataFlowMapReduceTree householdRootNode = new DataFlowMapReduceTree(
        List.of(values),
        root,
        ancestorStream.iterator()
    );
    final List<List<String>> outputRecords = new ArrayList<>();
    householdRootNode.reduce().forEachRemaining(rec -> outputRecords.add(rec));
    MatcherAssert.assertThat(outputRecords, Matchers.empty());
  }

  @Test
  public void testTwoEntityFilter() throws IOException {
    NumberRangeFilter<Long> rangeFilter = new NumberRangeFilter<>(
        "test",
        indiaICEMRStudy.getParticipantEntity(),
        indiaICEMRStudy.getTimeSinceLastMalaria(),
        60L,
        180L
    );
    NumberRangeFilter<Long> rootRangeFilter = new NumberRangeFilter<>(
        "test",
        indiaICEMRStudy.getHouseholdEntity(),
        indiaICEMRStudy.getPersonsInHousehold(),
        0L,
        10L
    );
    SubsettingJoinNode participantNode = new SubsettingJoinNode(
        List.of(binaryValuesStreamer.streamFilteredValues(rangeFilter, indiaICEMRStudy.getStudy())),
        Collections.emptyList(),
        indiaICEMRStudy.getParticipantEntity(),
        indiaICEMRStudy.getStudy(),
        entityIdIndexIteratorConverter
    );
    Iterator<VariableValueIdPair<?>> values = binaryValuesStreamer.streamIdValuePairs(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getPersonsInHousehold());
    Iterator<VariableValueIdPair<?>> secondColumn = binaryValuesStreamer.streamIdValuePairs(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getHealthFacilityDist());
    List<VariableValueIdPair<List<Long>>> ancestors = Collections.emptyList();

    SubsettingJoinNode root = new SubsettingJoinNode(
        List.of(binaryValuesStreamer.streamFilteredValues(rootRangeFilter, indiaICEMRStudy.getStudy())),
        List.of(participantNode),
        indiaICEMRStudy.getHouseholdEntity(),
        indiaICEMRStudy.getStudy(),
        entityIdIndexIteratorConverter);

    DataFlowMapReduceTree joiner = new DataFlowMapReduceTree(
        List.of(values, secondColumn),
        root,
        ancestors.iterator()
    );
    final List<List<String>> outputRecords = new ArrayList<>();
    joiner.reduce().forEachRemaining(rec -> outputRecords.add(rec));
    MatcherAssert.assertThat(outputRecords, Matchers.hasSize(38));
  }
}
