package org.veupathdb.service.eda.ss.model;

/* a brief version of the study */
public class StudyOverview {

  public enum StudySourceType {
    CURATED,
    USER_SUBMITTED
  }

  private final String id;
  private final String internalAbbrev;
  private final StudySourceType studySourceType;

  public StudyOverview(String id, String internalAbbrev, StudySourceType studySourceType) {
    this.id = id;
    this.internalAbbrev = internalAbbrev;
    this.studySourceType = studySourceType;
  }

  public String getStudyId() {
    return id;
  }

  public String getInternalAbbrev() {
    return internalAbbrev;
  }

  public StudySourceType getStudySourceType() {
    return studySourceType;
  }
}
