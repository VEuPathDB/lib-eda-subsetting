package org.veupathdb.service.eda.subset.model.variable.binary;

import java.nio.file.Path;

public interface StudyFinder {
  Path findStudyPath(String studyDir);
}
