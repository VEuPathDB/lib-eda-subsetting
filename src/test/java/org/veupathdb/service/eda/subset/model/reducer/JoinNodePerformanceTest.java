package org.veupathdb.service.eda.subset.model.reducer;

import org.gusdb.fgputil.iterator.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.subset.model.variable.binary.LongValueConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.ValueWithIdDeserializer;
import org.veupathdb.service.eda.subset.testutil.BinaryFileGenerator;
import org.veupathdb.service.eda.subset.testutil.TestDataProvider;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Tag("Performance")
public class JoinNodePerformanceTest {
  private static String directory;
  private static List<Path> files;

  private static ExecutorService threadPool;

  @BeforeAll
  public static void beforeAll() throws Exception {
    threadPool = Executors.newCachedThreadPool();
    directory = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath();
    files = new ArrayList<>();
    final File studyDir = new File(directory, TestDataProvider.STUDY_ID);
    studyDir.mkdir();
    final File entityDir = new File(studyDir.getAbsolutePath(), TestDataProvider.getNextEntityId());
    entityDir.mkdir();
    final int numFiles = Integer.parseInt(System.getProperty("numFiles", "10"));
    final int recordCount = Integer.parseInt(System.getProperty("recordCount", "2000000"));
    final boolean cached = Boolean.parseBoolean(System.getProperty("cached", "true"));
    IntStream.range(0, numFiles)
      .forEach(i -> {
        Path path = Path.of(directory, "var-file-" + i);
        files.add(path);
        BinaryFileGenerator.generate(path, recordCount, cached);
      });
  }

  @AfterAll
  public static void afterAll() {
    threadPool.shutdown();
    files.forEach(file -> file.toFile().deleteOnExit());
  }

  @Test
  public void run() throws Exception {
    List<CloseableIterator<Long>> filteredValueFiles = new ArrayList<>();
    final ValueWithIdDeserializer<Long> serializer = new ValueWithIdDeserializer<>(new LongValueConverter());
    for (Path path : files) {
      filteredValueFiles.add(
        new FilteredValueIterator<>(path, i -> true, serializer, VariableValueIdPair::getIdIndex, threadPool, threadPool));
    }
    CloseableIterator<Long> merger = new StreamIntersectMerger(filteredValueFiles);
    Instant start = Instant.now();
    while (merger.hasNext()) {
      merger.next();
    }
    Duration duration = Duration.between(start, Instant.now());
    long totalSize = files.stream()
      .map(file -> file.toFile().length()).mapToLong(l -> l).sum();

    System.out.println("TOTAL DURATION: " + duration.toMillis());
    System.out.println("TOTAL SIZE OF FILES (MB): " + (totalSize / 1024 / 1024));
  }
}
