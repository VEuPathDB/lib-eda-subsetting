package org.veupathdb.service.edass.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
import org.veupathdb.service.edass.model.HistogramTuple;
import org.veupathdb.service.edass.model.NumberRangeFilter;
import org.veupathdb.service.edass.model.NumberSetFilter;
import org.veupathdb.service.edass.model.StringSetFilter;
import org.veupathdb.service.edass.model.Study;
import org.veupathdb.service.edass.model.StudySubsettingUtils;
import org.veupathdb.service.edass.model.Variable;
import org.veupathdb.service.edass.generated.model.APIDateRangeFilter;
import org.veupathdb.service.edass.generated.model.APIDateSetFilter;
import org.veupathdb.service.edass.generated.model.APIFilter;
import org.veupathdb.service.edass.generated.model.APIHistogramTuple;
import org.veupathdb.service.edass.generated.model.APIHistogramTupleImpl;
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
  public GetStudiesEntitiesByStudyIdAndEntityIdResponse getStudiesEntitiesByStudyIdAndEntityId(String studyId,
      String entityId) {
    // TODO Auto-generated method stub
    return null;
  }

    @Override
  public PostStudiesEntitiesVariableSummaryByStudyIdAndEntityIdResponse postStudiesEntitiesVariableSummaryByStudyIdAndEntityId(String studyId,
      String entityId, EntityHistogramPostRequest request) {
      
    DataSource datasource = DbManager.applicationDatabase().getDataSource();
    
    // unpack data from API input to model objects
    List<String> vars = new ArrayList<String>();
    vars.add(request.getVariableId());  // force into a list for the unpacker
    Unpacked unpacked = unpack(datasource, studyId, entityId, request.getFilters(), vars);

    String varId = request.getVariableId();
    Variable var = unpacked.study.getVariable(varId).orElseThrow(() -> new BadRequestException("Variable ID not found: " + varId));

    // generate and run sql to get histogram tuples
    Iterator<HistogramTuple> tuples =
        StudySubsettingUtils.produceHistogramSubset(datasource, unpacked.study, unpacked.entity, var, unpacked.filters);

    // convert to stream for response
    Stream<APIHistogramTuple> apiTuplesStream = convertTuplesToStream(tuples);
    //TODO
   return null;
  }

  private Stream<APIHistogramTuple> convertTuplesToStream(Iterator<HistogramTuple> tuples) {
    Iterable<HistogramTuple> iterable = () -> tuples;
    Stream<HistogramTuple> targetStream = StreamSupport.stream(iterable.spliterator(), false);

    return targetStream.map(tuple -> {
      APIHistogramTuple apiTuple = new APIHistogramTupleImpl();
      apiTuple.setValue(tuple.getValue());
      apiTuple.setCount(tuple.getCount());
      return apiTuple;
    });
  }

  @Override
  public PostStudiesEntitiesTabularByStudyIdAndEntityIdResponse postStudiesEntitiesTabularByStudyIdAndEntityId(String studyId,
      String entityId, EntityTabularPostRequest request) {
    
    DataSource datasource = DbManager.applicationDatabase().getDataSource();
    
    Unpacked unpacked = unpack(datasource, studyId, entityId, request.getFilters(), request.getOutputVariableIds());

    StudySubsettingUtils.produceTabularSubset(datasource, unpacked.study, unpacked.entity,
        request.getOutputVariableIds(), unpacked.filters);

    return null;
  }
  
  private Unpacked unpack(DataSource datasource, String studyId, String entityId, List<APIFilter> apiFilters, List<String> variableIds) {
    String studIdStr = "Study ID " + studyId;
    if (!Study.validateStudyId(datasource, studyId))
      throw new NotFoundException(studIdStr + " is not found.");
   
    Study study = Study.loadStudy(datasource, studyId);
    Entity entity = study.getEntity(entityId).orElseThrow(() -> new NotFoundException("In " + studIdStr + " Entity ID not found: " + entityId));
    
    validateVariableIds(study, entityId, variableIds);

    List<Filter> filters = constructFiltersFromAPIFilters(study, apiFilters);
  
    return new Unpacked(study, entity, filters);
  }
  
  class Unpacked {
    Unpacked(Study study, Entity entity, List<Filter> filters) {
      this.study = study;
      this.entity = entity;
      this.filters = filters;
    }
    Study study;
    Entity entity;
    List<Filter> filters;
  }
  
  /*
   * Given a study and a set of API filters, construct and return a set of filters, each being the appropriate
   * filter subclass
   */
  static List<Filter> constructFiltersFromAPIFilters(Study study, List<APIFilter> filters) {
    List<Filter> subsetFilters = new ArrayList<Filter>();

    String errPrfx = "A filter references an unfound ";
    
    for (APIFilter apiFilter : filters) {
      
      // validate filter's entity id
      Supplier<BadRequestException> excep = 
          () -> new BadRequestException(errPrfx + "entity ID: " + apiFilter.getEntityId());
      Entity entity = study.getEntity(apiFilter.getEntityId()).orElseThrow(excep);
      
      // validate filter's variable id
      String varId = apiFilter.getVariableId();
      study.getVariable(varId).orElseThrow(() -> new BadRequestException("Variable '" + varId + "' is not found"));

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
  static void validateVariableIds(Study study, String outputEntityId, List<String> variableIds) {
    for (String varId : variableIds) {
      
      String errMsg = "Output variable '" + varId
          + "' is not found for entity with ID: '" + outputEntityId + "'";
      
      Variable var = study.getVariable(varId).orElseThrow(() -> new BadRequestException(errMsg));
      if (var.getEntityId().equals(outputEntityId)) throw new BadRequestException(errMsg);   
    }
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
