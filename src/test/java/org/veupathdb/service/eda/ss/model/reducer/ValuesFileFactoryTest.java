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
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.ValueWithIdSerializer;
import org.veupathdb.service.eda.ss.testutil.TestDataProvider;
import org.veupathdb.service.eda.ss.testutil.VariableWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class ValuesFileFactoryTest {
  private static String directory;
  private static ValuesFileFactory fileFactory;

  @BeforeAll
  public static void setup() throws Exception {
    directory = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath();
    fileFactory = new ValuesFileFactory(Path.of(directory));
    final File studyDir = new File(directory, TestDataProvider.STUDY_ID);
    studyDir.mkdir();
    final File entityDir = new File(studyDir.getAbsolutePath(), TestDataProvider.ENTITY_ID);
    entityDir.mkdir();
    Path varFile = Path.of(directory, TestDataProvider.STUDY_ID, TestDataProvider.ENTITY_ID, TestDataProvider.VARIABLE_ID);
    ValueWithIdSerializer<Long> serializer = new ValueWithIdSerializer<>(new LongValueConverter());
    try (VariableWriter<Long> writer = new VariableWriter<>(new FileOutputStream(varFile.toString()), serializer)) {
      writer.writeVar(new VariableValueIdPair<>(0L,0L));
      writer.writeVar(new VariableValueIdPair<>(1L,1L));
      writer.writeVar(new VariableValueIdPair<>(2L,2L));
      writer.writeVar(new VariableValueIdPair<>(3L,3L));
      writer.writeVar(new VariableValueIdPair<>(4L,4L));
      writer.writeVar(new VariableValueIdPair<>(5L,5L));
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
    Entity entity = TestDataProvider.constructEntity();
    IntegerVariable intVariable = TestDataProvider.constructIntVariable(entity);
    entity.addVariable(intVariable);
    NumberSetFilter<Long> filter = new NumberSetFilter<>("test", entity, intVariable, List.of(1L, 2L, 3L));
    Iterator<Long> valueFilter = fileFactory.createFromFilter(filter);
    MatcherAssert.assertThat(() -> valueFilter, Matchers.contains(1L, 2L, 3L));
  }

  @Test
  public void testNumberRangeFilter() throws IOException {
    Entity entity = TestDataProvider.constructEntity();
    IntegerVariable intVariable = TestDataProvider.constructIntVariable(entity);
    entity.addVariable(intVariable);
    NumberRangeFilter<Long> filter = new NumberRangeFilter<>("test", entity, intVariable, 1L, 3L);
    Iterator<Long> valueFilter = fileFactory.createFromFilter(filter);
    MatcherAssert.assertThat(() -> valueFilter, Matchers.contains(1L, 2L, 3L));
  }
}
