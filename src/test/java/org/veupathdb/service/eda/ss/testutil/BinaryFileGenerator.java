package org.veupathdb.service.eda.ss.testutil;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.converter.LongValueConverter;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueWithIdSerializer;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.stream.LongStream;

public class BinaryFileGenerator {

  public static void generate(Path path, int recordCount, boolean cached) {
    if (cached) {
      generateCachedFile(path, recordCount);
    } else {
      generateUncachedFile(path, recordCount);
    }
  }

  private static void generateCachedFile(Path path, int recordCount) {
    ValueWithIdSerializer<Long> serializer = new ValueWithIdSerializer<>(new LongValueConverter());
    try (VariableWriter<Long> writer = new VariableWriter<>(new FileOutputStream(path.toString()), serializer)) {
      LongStream.range(0, recordCount).forEach(i -> writer.writeVar(new VariableValueIdPair<>(i, i)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void generateUncachedFile(Path path, int recordCount) {
    Path cachedPath = Path.of(path.toString() + "-cached");
    generateCachedFile(cachedPath, recordCount);
    try {
      Process createFileProc = Runtime.getRuntime().exec(String.format(
          "dd if=%s of=%s bs=1M oflag=direct", cachedPath, path));
      createFileProc.waitFor();
      new File(cachedPath.toString()).deleteOnExit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
