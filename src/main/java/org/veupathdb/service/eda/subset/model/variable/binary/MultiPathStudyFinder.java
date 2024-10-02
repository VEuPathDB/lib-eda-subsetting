package org.veupathdb.service.eda.subset.model.variable.binary;

import org.gusdb.fgputil.IoUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Finds study files on a file system in accordance with paths configured to be available relative to the configured
 * root directory.
 */
public class MultiPathStudyFinder implements StudyFinder {
  private final List<String> availablePaths;
  private final Path root;
  private final Map<String, Path> studyIdToLocationCache;

  /**
   * Constructs an instance of a MultiPathStudyFinder.
   *
   * @param availablePaths The list of paths relative to the root directory in which studies should be discovered.
   *                       Since the root directory may contain data scoped outside the running instance of the
   *                       service, this argument is used to restrict traversal of external data.
   * @param root The root directory from which traversal of availablePaths should start.
   */
  public MultiPathStudyFinder(List<String> availablePaths, Path root) {
    this.availablePaths = availablePaths;
    this.root = root;
    this.studyIdToLocationCache = new HashMap<>();
  }

  /**
   * Find the path to the studyDir in accordance with configured available paths. The implementation sequentially
   * searches each available path in the order specified in {@link MultiPathStudyFinder#availablePaths}.
   *
   * @param studyDir Name of study directory to search.
   * @return Path of study directory if found.
   */
  @Override
  public Path findStudyPath(String studyDir) {
    Optional<Path> cachedStudyDir = lookupStudyDir(studyDir)
      .filter(path -> path.toFile().exists());
    if (cachedStudyDir.isPresent()) {
      return cachedStudyDir.get();
    }
    for (String availablePath : availablePaths) {
      Optional<Path> foundStudyPath = tryFindStudyPath(studyDir, availablePath);
      if (foundStudyPath.isPresent()) {
        cacheStudyDir(studyDir, foundStudyPath.get());
        return foundStudyPath.get();
      }
    }
    throw new RuntimeException("Could not find study directory: " + studyDir + " in any of the specified paths " + availablePaths);
  }

  private Optional<Path> tryFindStudyPath(String studyDir, String pattern) {
    String absolutePattern = String.join("/", root.toString(), pattern, studyDir);
    try {
      List<Path> paths = IoUtil.findDirsFromAbsoluteDirWithWildcards(absolutePattern);
      return paths.stream()
        .filter(Files::exists)
        .findFirst();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  private Optional<Path> lookupStudyDir(String studyDir) {
    return Optional.ofNullable(studyIdToLocationCache.get(studyDir));
  }

  private void cacheStudyDir(String studyDir, Path path) {
    studyIdToLocationCache.put(studyDir, path);
  }
}
