package org.veupathdb.service.eda.ss.model.variable.binary;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.IoUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Encapsulates the specification of an available path pattern. The enables a consumer to specify one or more directories
 * within a directory. A single directory can be specified with a {@link PatternType#DIRECT_MATCH} whereas multiple
 * directories can be specified via wildcard syntax with a {@link PatternType#GLOB}.
 */
public class PathPattern {
  private static final Logger LOG = LogManager.getLogger(PathPattern.class);

  private PatternType patternType;
  private String pattern;

  public PathPattern(String pattern) {
    final String[] patternParts = pattern.split(":");
    this.patternType = PatternType.fromPrefix(patternParts[0]);
    this.pattern = patternParts[1];
  }

  /**
   * Returns the path to the study specified by the studyDir parameter if it can be found traversing the root directory
   * matching against the pattern.
   * @param root Root directory of the search.
   * @param studyDir Study directory to search for.
   * @return Path if found, empty otherwise.
   */
  public Optional<Path> findStudy(String root, String studyDir) {
    if (patternType == PatternType.DIRECT_MATCH) {
      Path path = Path.of(root, pattern, studyDir);
      if (Files.exists(path) && Files.isDirectory(path)) {
        return Optional.of(path);
      }
      return Optional.empty();
    } else if (patternType == PatternType.GLOB) {
      String absolutePattern = String.join("/", root, pattern, studyDir);
      List<Path> paths;
      try {
        paths = IoUtil.findDirsFromAbsoluteDirWithWildcards(absolutePattern);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return paths.stream()
          .filter(path -> Files.exists(path))
          .findFirst();
    }
    return null;
  }


  public enum PatternType {
    GLOB("GLOB"),
    DIRECT_MATCH("PATH");

    private String prefix;

    PatternType(String prefix) {
      this.prefix = prefix;
    }

    public static PatternType fromPrefix(String prefix) {
      return Arrays.stream(values())
          .filter(patternType -> patternType.prefix.equals(prefix))
          .findFirst()
          .orElseThrow();
    }
  }
}
