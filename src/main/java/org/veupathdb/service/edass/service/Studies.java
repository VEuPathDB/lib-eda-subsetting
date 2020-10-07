package org.veupathdb.service.edass.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;

import org.veupathdb.lib.container.jaxrs.utils.db.DbManager;
import org.veupathdb.service.edass.generated.model.EntityHistogramPostRequest;
import org.veupathdb.service.edass.generated.model.EntityTabularPostRequest;
import org.veupathdb.service.edass.generated.model.StudiesGetResponseImpl;
import org.veupathdb.service.edass.model.DateRangeFilter;
import org.veupathdb.service.edass.model.DateSetFilter;
import org.veupathdb.service.edass.model.Entity;
import org.veupathdb.service.edass.model.Filter;
import org.veupathdb.service.edass.model.NumberRangeFilter;
import org.veupathdb.service.edass.model.NumberSetFilter;
import org.veupathdb.service.edass.model.StringSetFilter;
import org.veupathdb.service.edass.model.Study;
import org.veupathdb.service.edass.generated.model.APIDateRangeFilter;
import org.veupathdb.service.edass.generated.model.APIDateSetFilter;
import org.veupathdb.service.edass.generated.model.APIFilter;
import org.veupathdb.service.edass.generated.model.APINumberRangeFilter;
import org.veupathdb.service.edass.generated.model.APINumberSetFilter;
import org.veupathdb.service.edass.generated.model.APIStringSetFilter;
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
    
    if (!Study.validateStudyId(datasource, studyId))
      throw new NotFoundException("Study ID " + studyId + " is not found.");

   return null;
  }

  @Override
  public PostStudiesTabularByStudyIdAndEntityIdResponse postStudiesTabularByStudyIdAndEntityId(String studyId,
      String entityId, EntityTabularPostRequest request) {
    
    DataSource datasource = DbManager.applicationDatabase().getDataSource();
    
    if (!Study.validateStudyId(datasource, studyId))
      throw new NotFoundException("Study ID " + studyId + " is not found.");
   
    Study study = Study.loadStudy(datasource, studyId);
    validateOutputVariables(study, request.getOutputEntityId(), request.getOutputVariableIds());

   return null;
  }
  
  /*
   * Given a study and a set of API filters, construct and return a set of filters, each being the appropriate
   * filter subclass
   */
  static Set<Filter> constructFiltersFromAPIFilters(Study study, Set<APIFilter> filters) {
    Set<Filter> subsetFilters = new HashSet<Filter>();

    for (APIFilter apiFilter : filters) {
      Entity entity = study.getEntity(apiFilter.getEntityId());
      String varId = apiFilter.getVariableId();
      
      if (study.getVariable(varId) == null) 
        throw new BadRequestException("Variable '" + varId + "' is not found");

      Filter newFilter;
      if (apiFilter instanceof APIDateRangeFilter) {
        APIDateRangeFilter f = (APIDateRangeFilter)apiFilter;
        newFilter = new DateRangeFilter(entity, varId,
            convertDate(f.getMin()), convertDate(f.getMax()));
      } else if (apiFilter instanceof APIDateSetFilter) {
        APIDateSetFilter f = (APIDateSetFilter)apiFilter;
        List<LocalDateTime> dateSet = new ArrayList<LocalDateTime>();
        for (String dateStr : f.getDateSet()) dateSet.add(convertDate(dateStr));
        newFilter = new DateSetFilter(entity, varId, dateSet);
      } else if (apiFilter instanceof APINumberRangeFilter) {
        APINumberRangeFilter f = (APINumberRangeFilter)apiFilter;
        newFilter = new NumberRangeFilter(entity, varId, f.getMin(), f.getMax());
      } else if (apiFilter instanceof APINumberSetFilter) {
        APINumberSetFilter f = (APINumberSetFilter)apiFilter;
        newFilter = new NumberSetFilter(entity, varId, f.getNumberSet());
      } else if (apiFilter instanceof APIStringSetFilter) {
        APIStringSetFilter f = (APIStringSetFilter)apiFilter;
        newFilter = new StringSetFilter(entity, varId, f.getStringSet());
      } else
        throw new InternalServerErrorException("Input filter not an expected subclass of Filter");

      subsetFilters.add(newFilter);
    }
    return subsetFilters;
  }

  /* confirm that output variables belong to the output entity */
  static void validateOutputVariables(Study study, String outputEntityId, List<String> outputVariableNames) {
    for (String varId : outputVariableNames) 
      if (!study.getVariable(varId).getEntityId().equals(outputEntityId))
        throw new BadRequestException("Output variable '" + varId
            + "' is not consistent with output entity '" + outputEntityId + "'" );    
  }
  
  static LocalDateTime convertDate(String dateStr) {
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
      LocalDateTime date = LocalDateTime.parse(dateStr, formatter); 
      return date;
    } catch (DateTimeParseException e) {
      throw new BadRequestException("Can't parse date string" + dateStr);
    }
  }
}
