package org.veupathdb.service.edass.service;

import java.util.ArrayList;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Request;

import org.veupathdb.service.edass.generated.model.StudiesGetResponseImpl;
import org.veupathdb.service.edass.generated.model.StudyOverview;

public class Studies implements org.veupathdb.service.edass.generated.resources.Studies {

  @Context
  private Request req;

 @Override
  public GetStudiesResponse getStudies() {
   var out = new StudiesGetResponseImpl();
   out.setStudies(new ArrayList<StudyOverview>());
   return GetStudiesResponse.respond200WithApplicationJson(out);
 }

  @Override
  public GetStudiesStudiesByStudyIdResponse getStudiesStudiesByStudyId(String studyId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetStudiesStudiesStudiesByStudyIdAndEntityIdResponse getStudiesStudiesStudiesByStudyIdAndEntityId(
      String studyId, String entityId) {
    // TODO Auto-generated method stub
    return null;
  }

}
