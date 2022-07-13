package org.veupathdb.service.eda.ss.model.reducer;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.filter.NumberRangeFilter;
import org.veupathdb.service.eda.ss.model.reducer.ancestor.EntityIdIndexIteratorConverter;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.ss.testutil.IndiaICEMRStudy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class EntityJoinerRootTest {
  private Path binaryDirectory = Path.of(System.getProperty("user.dir"), "src/test/resources");
  private BinaryFilesManager binaryFilesManager = new BinaryFilesManager(binaryDirectory);
  private EntityIdIndexIteratorConverter entityIdIndexIteratorConverter = new EntityIdIndexIteratorConverter(binaryFilesManager);
  private ValuesIteratorFactory valuesIteratorFactory = new ValuesIteratorFactory(binaryDirectory);
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
        List.of(valuesIteratorFactory.createFromFilter(sampleRangeFilter, indiaICEMRStudy.getStudy())),
        Collections.emptyList(),
        indiaICEMRStudy.getSampleEntity(),
        indiaICEMRStudy.getStudy(),
        entityIdIndexIteratorConverter
    );
    SubsettingJoinNode participantNode = new SubsettingJoinNode(
        List.of(valuesIteratorFactory.createFromFilter(timeSinceLastMalariaFilter, indiaICEMRStudy.getStudy())),
        List.of(sampleNode),
        indiaICEMRStudy.getParticipantEntity(),
        indiaICEMRStudy.getStudy(),
        entityIdIndexIteratorConverter
    );
    Iterator<VariableValueIdPair<?>> values = valuesIteratorFactory.createFromVariable(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getPersonsInHousehold());
    EntityJoinerRoot householdRootNode = new EntityJoinerRoot(
        List.of(valuesIteratorFactory.createFromFilter(personsInHouseholdFilter, indiaICEMRStudy.getStudy())),
        List.of(values),
        List.of(participantNode),
        indiaICEMRStudy.getHouseholdEntity(),
        indiaICEMRStudy.getStudy(),
        entityIdIndexIteratorConverter
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
        List.of(valuesIteratorFactory.createFromFilter(rangeFilter, indiaICEMRStudy.getStudy())),
        Collections.emptyList(),
        indiaICEMRStudy.getParticipantEntity(),
        indiaICEMRStudy.getStudy(),
        entityIdIndexIteratorConverter
    );
    Iterator<VariableValueIdPair<?>> values = valuesIteratorFactory.createFromVariable(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getPersonsInHousehold());
    Iterator<VariableValueIdPair<?>> secondColumn = valuesIteratorFactory.createFromVariable(indiaICEMRStudy.getStudy(), indiaICEMRStudy.getHealthFacilityDist());

    EntityJoinerRoot joiner = new EntityJoinerRoot(
        List.of(valuesIteratorFactory.createFromFilter(rootRangeFilter, indiaICEMRStudy.getStudy())),
        List.of(values, secondColumn),
        List.of(participantNode),
        indiaICEMRStudy.getHouseholdEntity(),
        indiaICEMRStudy.getStudy(),
        entityIdIndexIteratorConverter
    );
    final List<List<String>> outputRecords = new ArrayList<>();
    joiner.reduce().forEachRemaining(rec -> outputRecords.add(rec));
    MatcherAssert.assertThat(outputRecords, Matchers.hasSize(38));
  }

  private VariableValueIdPair<String> constructPair(long idIndex, String value) {
    return new VariableValueIdPair<>(idIndex, value);
  }
}
