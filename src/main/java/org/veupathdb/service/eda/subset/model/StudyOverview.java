package org.veupathdb.service.eda.subset.model;

import java.util.Date;

/* a brief version of the study */
public class StudyOverview {

  public enum StudySourceType {
    CURATED,
    USER_SUBMITTED
  }

  private final String id;
  private final String internalAbbrev;
  private final StudySourceType studySourceType;
  private final Date lastModified;

  public StudyOverview(String id,
                       String internalAbbrev,
                       StudySourceType studySourceType,
                       Date lastModified) {
    this.id = id;
    this.internalAbbrev = internalAbbrev;
    this.studySourceType = studySourceType;
    this.lastModified = lastModified;
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

  public Date getLastModified() {
    return lastModified;
  }
}
