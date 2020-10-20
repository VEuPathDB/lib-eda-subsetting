package org.veupathdb.service.edass.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import javax.sql.DataSource;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;

import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.lib.container.jaxrs.utils.db.DbManager;
import org.veupathdb.service.edass.generated.model.VariableDistributionPostRequest;
import org.veupathdb.service.edass.generated.model.VariableDistributionPostResponseStream;
import org.veupathdb.service.edass.generated.model.EntityTabularPostRequest;
import org.veupathdb.service.edass.generated.model.EntityTabularPostResponseStream;
import org.veupathdb.service.edass.generated.model.StudiesGetResponseImpl;
import org.veupathdb.service.edass.generated.model.StudyIdGetResponse;
import org.veupathdb.service.edass.generated.model.StudyIdGetResponseImpl;
import org.veupathdb.service.edass.generated.model.VariableCountPostRequest;
import org.veupathdb.service.edass.generated.model.VariableCountPostResponse;
import org.veupathdb.service.edass.generated.model.VariableCountPostResponseImpl;
import org.veupathdb.service.edass.model.DateRangeFilter;
import org.veupathdb.service.edass.model.DateSetFilter;
import org.veupathdb.service.edass.model.Entity;
import org.veupathdb.service.edass.model.EntityResultSetUtils;
import org.veupathdb.service.edass.model.Filter;
import org.veupathdb.service.edass.model.NumberRangeFilter;
import org.veupathdb.service.edass.model.NumberSetFilter;
import org.veupathdb.service.edass.model.StringSetFilter;
import org.veupathdb.service.edass.model.Study;
import org.veupathdb.service.edass.model.StudySubsettingUtils;
import org.veupathdb.service.edass.model.Variable;
import org.veupathdb.service.edass.generated.model.APIDateRangeFilter;
import org.veupathdb.service.edass.generated.model.APIDateSetFilter;
import org.veupathdb.service.edass.generated.model.APIEntity;
import org.veupathdb.service.edass.generated.model.APIEntityImpl;
import org.veupathdb.service.edass.generated.model.APIFilter;
import org.veupathdb.service.edass.generated.model.APINumberRangeFilter;
import org.veupathdb.service.edass.generated.model.APINumberSetFilter;
import org.veupathdb.service.edass.generated.model.APIStringSetFilter;
import org.veupathdb.service.edass.generated.model.APIStudyDetail;
import org.veupathdb.service.edass.generated.model.APIStudyDetailImpl;
import org.veupathdb.service.edass.generated.model.APIStudyOverview;
import org.veupathdb.service.edass.generated.model.EntityCountPostRequest;
import org.veupathdb.service.edass.generated.model.EntityCountPostResponse;
import org.veupathdb.service.edass.generated.model.EntityCountPostResponseImpl;

public class Studies implements org.veupathdb.service.edass.generated.resources.Studies {

  @Override
  public GetStudiesResponse getStudies() {
    var out = new StudiesGetResponseImpl();
    out.setStudies(new ArrayList<APIStudyOverview>());
    return GetStudiesResponse.respond200WithApplicationJson(out);
  }

  @Override
  public GetStudiesByStudyIdResponse getStudiesByStudyId(String studyId) {

    DataSource datasource = DbManager.applicationDatabase().getDataSource();

    TreeNode<Entity> entityTree = EntityResultSetUtils.getStudyEntityTree(datasource, studyId);

    APIEntity apiEntityTree = entityTreeToAPITree(entityTree);
    APIStudyDetail study = new APIStudyDetailImpl();
    // TODO: lose or fill in study.setName() prop
    study.setId(studyId);
    study.setRootEntity(apiEntityTree);
    
    StudyIdGetResponse response = new StudyIdGetResponseImpl();
    response.setStudy(study);
    
    return GetStudiesByStudyIdResponse.respond200WithApplicationJson(response);
  }
  
  private static APIEntity entityTreeToAPITree(TreeNode<Entity> root) {
    return root.mapStructure((entity, mappedChildren) -> {
      APIEntity apiEntity = new APIEntityImpl();
      apiEntity.setDescription(entity.getDescription());
      apiEntity.setName(entity.getName());
      apiEntity.setId(entity.getId());
      apiEntity.setChildren(mappedChildren);
      // TODO add variables
      return apiEntity;
    });
  }

  @Override
  public GetStudiesEntitiesByStudyIdAndEntityIdResponse getStudiesEntitiesByStudyIdAndEntityId(String studyId,
      String entityId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PostStudiesEntitiesVariablesDistributionByStudyIdAndEntityIdAndVariableIdResponse 
  postStudiesEntitiesVariablesDistributionByStudyIdAndEntityIdAndVariableId(
String studyId, String entityId, String variableId, VariableDistributionPostRequest request) {
      
    DataSource datasource = DbManager.applicationDatabase().getDataSource();
    
    // unpack data from API input to model objects
    List<String> vars = new ArrayList<String>();
    vars.add(variableId);  // force into a list for the unpacker
    UnpackedRequest unpacked = unpack(datasource, studyId, entityId, request.getFilters(), vars);

    String varId = request.getVariableId();
    Variable var = unpacked.study.getVariable(varId).orElseThrow(() -> new BadRequestException("Variable ID not found: " + varId));

    VariableDistributionPostResponseStream streamer = new VariableDistributionPostResponseStream
        (outStream -> StudySubsettingUtils.produceVariableDistribution(datasource, unpacked.study, unpacked.entity, var, unpacked.filters, outStream));

    return PostStudiesEntitiesVariablesDistributionByStudyIdAndEntityIdAndVariableIdResponse.
        respond200WithApplicationJson(streamer);
   }

    @Override
    public PostStudiesEntitiesVariablesCountByStudyIdAndEntityIdAndVariableIdResponse postStudiesEntitiesVariablesCountByStudyIdAndEntityIdAndVariableId(
        String studyId, String entityId, String variableId, VariableCountPostRequest request) {

      DataSource datasource = DbManager.applicationDatabase().getDataSource();

      // unpack data from API input to model objects
      List<String> vars = new ArrayList<String>();
      vars.add(variableId);  // force into a list for the unpacker
      UnpackedRequest unpacked = unpack(datasource, studyId, entityId, request.getFilters(), vars);

      Variable var = unpacked.study.getVariable(variableId).orElseThrow(() -> new BadRequestException("Variable ID not found: " + variableId));

      Integer count = StudySubsettingUtils.getVariableCount(datasource, unpacked.study, unpacked.entity,
          var, unpacked.filters);

      VariableCountPostResponse response = new VariableCountPostResponseImpl();
      response.setCount(count);

      return  PostStudiesEntitiesVariablesCountByStudyIdAndEntityIdAndVariableIdResponse.respond200WithApplicationJson(response);
    }

    @Override
  public PostStudiesEntitiesTabularByStudyIdAndEntityIdResponse postStudiesEntitiesTabularByStudyIdAndEntityId(String studyId,
      String entityId, EntityTabularPostRequest request) {
    
    DataSource datasource = DbManager.applicationDatabase().getDataSource();
    
    UnpackedRequest unpacked = unpack(datasource, studyId, entityId, request.getFilters(), request.getOutputVariableIds());

    EntityTabularPostResponseStream streamer = new EntityTabularPostResponseStream
        (outStream -> StudySubsettingUtils.produceTabularSubset(datasource, unpacked.study, unpacked.entity,
            request.getOutputVariableIds(), unpacked.filters, outStream));

    return PostStudiesEntitiesTabularByStudyIdAndEntityIdResponse.
        respond200WithApplicationJson(streamer);
  }
    
  @Override
  public PostStudiesEntitiesCountByStudyIdAndEntityIdResponse postStudiesEntitiesCountByStudyIdAndEntityId(
      String studyId, String entityId, EntityCountPostRequest request) {

    DataSource datasource = DbManager.applicationDatabase().getDataSource();

    // unpack data from API input to model objects
    List<String> vars = new ArrayList<String>();
    UnpackedRequest unpacked = unpack(datasource, studyId, entityId, request.getFilters(), vars);

    Integer count = StudySubsettingUtils.getEntityCount(datasource, unpacked.study, unpacked.entity,
        unpacked.filters);

    EntityCountPostResponse response = new EntityCountPostResponseImpl();
    response.setCount(count);

    return  PostStudiesEntitiesCountByStudyIdAndEntityIdResponse.respond200WithApplicationJson(response);
  }
  
  private UnpackedRequest unpack(DataSource datasource, String studyId, String entityId, List<APIFilter> apiFilters, List<String> variableIds) {
    String studIdStr = "Study ID " + studyId;
    if (!Study.validateStudyId(datasource, studyId))
      throw new NotFoundException(studIdStr + " is not found.");
   
    Study study = Study.loadStudy(datasource, studyId);
    Entity entity = study.getEntity(entityId).orElseThrow(() -> new NotFoundException("In " + studIdStr + " Entity ID not found: " + entityId));
    
    validateVariableIds(study, entityId, variableIds);

    List<Filter> filters = constructFiltersFromAPIFilters(study, apiFilters);
  
    return new UnpackedRequest(study, entity, filters);
  }
  
  class UnpackedRequest {
    UnpackedRequest(Study study, Entity entity, List<Filter> filters) {
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
