package org.veupathdb.service.eda.subset.model.db;

import org.gusdb.fgputil.ListBuilder;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.StudyOverview;

import java.util.List;

/**
 * Subclass of StudyProvider which knows how to provide studies from
 * multiple sources; it will choose the source based on the type of
 * study being requested (i.e. curated vs user study).
 */
public class StudyResolver implements StudyProvider {

  private final StudyProvider _curatedStudyProvider;
  private final StudyProvider _userStudyProvider;

  public StudyResolver(
      StudyProvider curatedStudyProvider,
      StudyProvider userStudyProvider) {
    _curatedStudyProvider = curatedStudyProvider;
    _userStudyProvider = userStudyProvider;
  }

  @Override
  public List<StudyOverview> getStudyOverviews() {
    return new ListBuilder<StudyOverview>()
      .addAll(_curatedStudyProvider.getStudyOverviews())
      .addAll(_userStudyProvider.getStudyOverviews())
      .toList();
  }

  @Override
  public Study getStudyById(String studyId) {
    return getStudyOverviews().stream()
      .filter(overview -> overview.getStudyId().equals(studyId))
      .findFirst()
      .map(overview -> switch (overview.getStudySourceType()) {
        case USER_SUBMITTED -> _userStudyProvider.getStudyById(overview.getStudyId());
        case CURATED -> _curatedStudyProvider.getStudyById(overview.getStudyId());
      })
      .orElseThrow(notFound(studyId));
  }
}
