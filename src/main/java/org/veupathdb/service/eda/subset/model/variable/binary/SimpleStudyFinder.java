package org.veupathdb.service.eda.subset.model.variable.binary;

import java.nio.file.Path;

public class SimpleStudyFinder implements StudyFinder {
  private final String root;

  public SimpleStudyFinder(String root) {
    this.root = root;
  }

  @Override
  public Path findStudyPath(String studyDir) {
    return Path.of(root, studyDir);
  }
}
