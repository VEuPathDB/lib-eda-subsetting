package org.veupathdb.service.eda.ss.model;

/* a brief version of the study */
public class StudyOverview {

  private final String id;
  private final String internalAbbrev;
  private final boolean isUserStudy;

  public StudyOverview(String id, String internalAbbrev, boolean isUserStudy) {
    this.id = id;
    this.internalAbbrev = internalAbbrev;
    this.isUserStudy = isUserStudy;
  }

  public String getStudyId() {
    return id;
  }

  public String getInternalAbbrev() {
    return internalAbbrev;
  }

  public boolean isUserStudy() {
    return isUserStudy;
  }
}
