package org.veupathdb.service.eda.ss.model.reducer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.ValueWithIdDeserializer;
import org.veupathdb.service.eda.ss.model.variable.binary.ValueWithIdSerializer;
import org.veupathdb.service.eda.ss.testutil.VariableWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class FilteredValueIteratorTest {
  private static final long SEQUENCE_SIZE = 1000L;
  private static final long NUM_DUPLICATES = 10L;

  private static File sequenceFile;
  private static File sequenceFileWithoutDuplicates;


  @BeforeAll
  public static void beforeAll() throws IOException {
    String dir = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath();
    FileOutputStream outputStream = new FileOutputStream(Path.of(dir, "test-sequence").toString());
    sequenceFile = Path.of(dir, "test-sequence").toFile();
    sequenceFile.deleteOnExit();
    FileOutputStream withoutDupesOutputStream = new FileOutputStream(Path.of(dir, "test-sequence-wo-dupes").toString());
    sequenceFileWithoutDuplicates = Path.of(dir, "test-sequence-wo-dupes").toFile();
    sequenceFileWithoutDuplicates.deleteOnExit();

    VariableWriter<Long> writer = new VariableWriter(outputStream, new ValueWithIdSerializer(new LongValueConverter()));
    for (long i = 0; i < SEQUENCE_SIZE; i++) {
      for (long j = 0; j < NUM_DUPLICATES; j++) {
        writer.writeVar(new VariableValueIdPair<>(i, i * NUM_DUPLICATES + j));
      }
    }

    VariableWriter<Long> writeWithoutDupes = new VariableWriter(withoutDupesOutputStream, new ValueWithIdSerializer(new LongValueConverter()));
    for (long i = 0; i < SEQUENCE_SIZE; i++) {
      writeWithoutDupes.writeVar(new VariableValueIdPair<>(i, i));
    }
  }

//  @Test
//  public void testIncludeDuplicates() throws IOException {
//    Iterator<Long> iterator = new FilteredValueIterator<>(
//        sequenceFile.toPath(),
//        x -> true,
//        new ValueWithIdDeserializer<>(new LongValueConverter()),
//        VariableValueIdPair::getValue,
//        FilteredValueIterator.DuplicateMode.INCLUDE);
//    final AtomicInteger count = new AtomicInteger(0);
////    iterator.forEachRemaining(System.out::println);
//
//    iterator.forEachRemaining(i -> count.incrementAndGet());
//    Assertions.assertEquals(10000, count.get());
//  }
//
//  @Test
//  public void testExcludeDuplicates() throws IOException {
//    Iterator<Long> iterator = new FilteredValueIterator<>(
//        sequenceFile.toPath(),
//        x -> true,
//        new ValueWithIdDeserializer<>(new LongValueConverter()),
//        VariableValueIdPair::getIdIndex,
//        FilteredValueIterator.DuplicateMode.EXCLUDE);
//    final AtomicInteger count = new AtomicInteger(0);
////    iterator.forEachRemaining(i -> count.incrementAndGet());
//        iterator.forEachRemaining(System.out::println);
//
////    Assertions.assertEquals(1000, count.get());
//  }
//
//  @Test
//  public void testExcludeDuplicatesWithFilters() throws IOException {
//    Iterator<Long> iterator = new FilteredValueIterator<>(
//        sequenceFile.toPath(),
//        x -> x == 198 || x == 199 || x == 232,
//        new ValueWithIdDeserializer<>(new LongValueConverter()),
//        VariableValueIdPair::getIdIndex,
//        FilteredValueIterator.DuplicateMode.EXCLUDE);
//    Assertions.assertEquals(19, iterator.next());
//    Assertions.assertEquals(23, iterator.next());
//  }
//
//  @Test
//  public void testExcludeDuplicatesWithFewFilters() throws IOException {
//    Iterator<VariableValueIdPair> iterator = new FilteredValueIterator<>(
//        sequenceFile.toPath(),
//        x -> !(x >= 101 && x <= 121),
//        new ValueWithIdDeserializer<>(new LongValueConverter()),
//        i -> i,
//        FilteredValueIterator.DuplicateMode.EXCLUDE);
//    final AtomicInteger count = new AtomicInteger(0);
//    iterator.forEachRemaining(i -> count.incrementAndGet());
//    Assertions.assertEquals(999, count.get());
//  }
//
//  @Test
//  public void testExcludeDuplicatesWithFiltersLastInSequence() throws IOException {
//    Iterator<Long> iterator = new FilteredValueIterator<>(
//        sequenceFile.toPath(),
//        x -> x == 9999L,
//        new ValueWithIdDeserializer<>(new LongValueConverter()),
//        VariableValueIdPair::getIdIndex,
//        FilteredValueIterator.DuplicateMode.EXCLUDE);
//    Assertions.assertEquals(999, iterator.next());
//  }
//
//  @Test
//  public void testExcludeDuplicatesInFileWithoutDupes() throws IOException {
//    Iterator<Long> iterator = new FilteredValueIterator<>(
//        sequenceFileWithoutDuplicates.toPath(),
//        x -> x == 50 || x == 51,
//        new ValueWithIdDeserializer<>(new LongValueConverter()),
//        VariableValueIdPair::getIdIndex,
//        FilteredValueIterator.DuplicateMode.EXCLUDE);
//    Assertions.assertEquals(50, iterator.next());
//    Assertions.assertEquals(51, iterator.next());
//  }
}
