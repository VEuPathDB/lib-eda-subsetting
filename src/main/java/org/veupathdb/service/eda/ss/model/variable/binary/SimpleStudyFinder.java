package org.veupathdb.service.eda.ss.model.variable.binary;

import java.nio.file.Path;

public class SimpleStudyFinder implements StudyFinder {
  private String root;

  public SimpleStudyFinder(String root) {
    this.root = root;
  }

  @Override
  public Path findStudyPath(String studyDir) {
    return Path.of(root, studyDir);
  }
}
