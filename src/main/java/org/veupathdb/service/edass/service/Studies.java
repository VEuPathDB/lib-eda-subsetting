package org.veupathdb.service.edass.service;

import java.util.ArrayList;

import javax.sql.DataSource;

import org.veupathdb.lib.container.jaxrs.utils.db.DbManager;
import org.veupathdb.service.edass.generated.model.EntityHistogramPostRequest;
import org.veupathdb.service.edass.generated.model.EntityTabularPostRequest;
import org.veupathdb.service.edass.generated.model.StudiesGetResponseImpl;
import org.veupathdb.service.edass.generated.model.APIStudyOverview;

public class Studies implements org.veupathdb.service.edass.generated.resources.Studies {

  @Override
  public GetStudiesResponse getStudies() {
    var out = new StudiesGetResponseImpl();
    out.setStudies(new ArrayList<APIStudyOverview>());
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
  public PostStudiesHistogramByStudyIdAndEntityIdResponse postStudiesHistogramByStudyIdAndEntityId(String studyId,
      String entityId, EntityHistogramPostRequest request) {
    DataSource datasource = DbManager.applicationDatabase().getDataSource();
   
   return null;
  }

  @Override
  public PostStudiesTabularByStudyIdAndEntityIdResponse postStudiesTabularByStudyIdAndEntityId(String studyId,
      String entityId, EntityTabularPostRequest request) {
    DataSource datasource = DbManager.applicationDatabase().getDataSource();
   
   return null;
  }

}
