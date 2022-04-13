package org.veupathdb.service.eda.ss.model;

/* a brief version of the study */
public class StudyOverview {

  private final String id;
  private final String internalAbbrev;

  public StudyOverview(String id, String internalAbbrev) {
    this.id = id;
    this.internalAbbrev = internalAbbrev;
  }

  public String getStudyId() {
    return id;
  }

  public String getInternalAbbrev() {
    return internalAbbrev;
  }
}
