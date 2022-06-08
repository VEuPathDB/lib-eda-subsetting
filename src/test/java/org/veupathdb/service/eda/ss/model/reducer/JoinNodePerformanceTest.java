package org.veupathdb.service.eda.ss.model.reducer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.variable.converter.LongValueConverter;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueWithIdSerializer;
import org.veupathdb.service.eda.ss.testutil.BinaryFileGenerator;
import org.veupathdb.service.eda.ss.testutil.TestDataProvider;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Tag("Performance")
public class JoinNodePerformanceTest {
  private static String directory;
  private static List<Path> files;

  @BeforeAll
  public static void beforeAll() throws Exception {
    directory = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath();
    files = new ArrayList<>();
    final File studyDir = new File(directory, TestDataProvider.STUDY_ID);
    studyDir.mkdir();
    final File entityDir = new File(studyDir.getAbsolutePath(), TestDataProvider.ENTITY_ID);
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
    files.forEach(file -> file.toFile().deleteOnExit());
  }

  @Test
  public void run() throws Exception{
    List<FilteredValueFile<?>> filteredValueFiles = new ArrayList<>();
    for (Path path: files) {
      filteredValueFiles.add(
          new FilteredValueFile<Long>(path, i -> true, new ValueWithIdSerializer(new LongValueConverter())));
    }
    SubsettingJoinNode node = new SubsettingJoinNode(filteredValueFiles);
    Iterator<Long> iterator = node.reduce();
    Instant start = Instant.now();
    while (iterator.hasNext()) {
      iterator.next();
    }
    Duration duration = Duration.between(start, Instant.now());
    long totalSize = files.stream()
        .map(file -> file.toFile().length())
        .collect(Collectors.summingLong(l -> l));

    System.out.println("TOTAL DURATION: " + duration.toMillis());
    System.out.println("TOTAL SIZE OF FILES (MB): " + (totalSize / 1024 / 1024));
  }
}
