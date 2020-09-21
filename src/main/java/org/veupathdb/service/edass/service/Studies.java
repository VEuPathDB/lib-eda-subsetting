package org.veupathdb.service.edass.service;

import java.util.ArrayList;

import javax.sql.DataSource;

import org.veupathdb.lib.container.jaxrs.utils.db.DbManager;
import org.veupathdb.service.edass.generated.model.EntityIdPostRequest;
import org.veupathdb.service.edass.generated.model.StudiesGetResponseImpl;
import org.veupathdb.service.edass.generated.model.StudyOverview;

public class Studies implements org.veupathdb.service.edass.generated.resources.Studies {

  @Override
  public GetStudiesResponse getStudies() {
    var out = new StudiesGetResponseImpl();
    out.setStudies(new ArrayList<StudyOverview>());
    return GetStudiesResponse.respond200WithApplicationJson(out);
  }

  @Override
  public GetStudiesByStudyIdResponse getStudiesByStudyId(String studyId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetStudiesByStudyIdAndEntityIdResponse getStudiesByStudyIdAndEntityId(String studyId,
      String entityId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PostStudiesByStudyIdAndEntityIdResponse postStudiesByStudyIdAndEntityId(String studyId,
      String entityId, EntityIdPostRequest request) {
    DataSource datasource = DbManager.applicationDatabase().getDataSource();
   
   return null;
  }

}
