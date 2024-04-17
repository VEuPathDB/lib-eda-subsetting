package org.veupathdb.service.eda.subset.model.db;

import jakarta.ws.rs.NotFoundException;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.StudyOverview;

import java.util.List;
import java.util.function.Supplier;

/**
 * Interface to provide metadata about studies.
 */
public interface StudyProvider {

  List<StudyOverview> getStudyOverviews();

  Study getStudyById(String studyId);

  default Supplier<NotFoundException> notFound(String studyId) {
    return () -> new NotFoundException("Study ID '" + studyId + "' not found:");
  }
}
