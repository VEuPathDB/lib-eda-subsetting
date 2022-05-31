package org.veupathdb.service.eda.ss.model.reducer;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.filter.NumberRangeFilter;
import org.veupathdb.service.eda.ss.model.filter.NumberSetFilter;
import org.veupathdb.service.eda.ss.model.variable.*;
import org.veupathdb.service.eda.ss.model.variable.serializer.IntValueSerializer;
import org.veupathdb.service.eda.ss.model.variable.serializer.ValueWithIdSerializer;
import org.veupathdb.service.eda.ss.testutil.VariableWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class ValuesFileFactoryTest {
  private static final String STUDY_ID = "GEMS1A";
  private static final String ENTITY_ID = "EUPA_12345";
  private static final String VARIABLE_ID = "EUPA_22222";

  private static String directory;
  private static ValuesFileFactory fileFactory;

  @BeforeAll
  public static void setup() throws Exception {
    directory = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath();
    fileFactory = new ValuesFileFactory(Path.of(directory));
    final File studyDir = new File(directory, STUDY_ID);
    studyDir.mkdir();
    final File entityDir = new File(studyDir.getAbsolutePath(), ENTITY_ID);
    entityDir.mkdir();
    Path varFile = Path.of(directory, STUDY_ID, ENTITY_ID, VARIABLE_ID);
    ValueWithIdSerializer<Integer> serializer = new ValueWithIdSerializer<>(new IntValueSerializer());
    try (VariableWriter<Integer, Integer> writer = new VariableWriter<>(new FileOutputStream(varFile.toString()), serializer)) {
      writer.writeVar(new VariableValue<>(0,0));
      writer.writeVar(new VariableValue<>(1,1));
      writer.writeVar(new VariableValue<>(2,2));
      writer.writeVar(new VariableValue<>(3,3));
      writer.writeVar(new VariableValue<>(4,4));
      writer.writeVar(new VariableValue<>(5,5));
    }
  }

  @AfterAll
  public static void cleanup() throws IOException {
    try (Stream<Path> walk = Files.walk(Path.of(directory))) {
      walk.sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .peek(System.out::println)
          .forEach(File::delete);
    }
  }

  @Test
  public void testNumberSetFilter() throws IOException {
    Entity entity = constructEntity();
    IntegerVariable intVariable = constructIntVariable(entity);
    entity.addVariable(intVariable);
    NumberSetFilter filter = new NumberSetFilter("test", entity, intVariable, List.of(1, 2, 3));
    MatcherAssert.assertThat(fileFactory.createFromSingleValue(filter), Matchers.contains(1, 2, 3));
  }

  @Test
  public void testNumberRangeFilter() throws IOException {
    Entity entity = constructEntity();
    IntegerVariable intVariable = constructIntVariable(entity);
    entity.addVariable(intVariable);
    NumberRangeFilter<Long> filter = new NumberRangeFilter<>("test", entity, intVariable, 1, 3);
    MatcherAssert.assertThat(fileFactory.createFromSingleValue(filter), Matchers.contains(1, 2, 3));
  }
  private Entity constructEntity() {
    return new Entity(
        ENTITY_ID,
        STUDY_ID,
        "My Study",
        "My Studies",
        "My favority study",
        "Mine",
        0L,
        false,
        false);
  }

  private IntegerVariable constructIntVariable(Entity entity) {
    return new IntegerVariable(
        new Variable.Properties(
            "label",
            VARIABLE_ID,
            entity,
            null,
            null,
            0L,
            "50",
            null,
            null
        ),
        new VariableWithValues.Properties(
            VariableType.INTEGER,
            null,
            Collections.emptyList(),
            0L,
            false,
            false,
            false,
            false,
            false
        ),
        null,
        new IntegerVariable.Properties("bleep bloops")
    );
  }
}