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
import org.veupathdb.service.edass.Resources;
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
import org.veupathdb.service.edass.model.Filter;
import org.veupathdb.service.edass.model.NumberRangeFilter;
import org.veupathdb.service.edass.model.NumberSetFilter;
import org.veupathdb.service.edass.model.StringSetFilter;
import org.veupathdb.service.edass.model.Study;
import org.veupathdb.service.edass.model.StudySubsettingUtils;
import org.veupathdb.service.edass.model.Variable;
import org.veupathdb.service.edass.model.Variable.VariableType;
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
import org.veupathdb.service.edass.generated.model.APIVariable;
import org.veupathdb.service.edass.generated.model.APIDateVariable;
import org.veupathdb.service.edass.generated.model.APIDateVariableImpl;
import org.veupathdb.service.edass.generated.model.APIStringVariable;
import org.veupathdb.service.edass.generated.model.APIStringVariableImpl;
import org.veupathdb.service.edass.generated.model.APINumberVariable;
import org.veupathdb.service.edass.generated.model.APINumberVariableImpl;
import org.veupathdb.service.edass.generated.model.EntityCountPostRequest;
import org.veupathdb.service.edass.generated.model.EntityCountPostResponse;
import org.veupathdb.service.edass.generated.model.EntityCountPostResponseImpl;

public class Studies implements org.veupathdb.service.edass.generated.resources.Studies {

  @Override
  public GetStudiesResponse getStudies() {
    var out = new StudiesGetResponseImpl();
    out.setStudies(new ArrayList<>());
    return GetStudiesResponse.respond200WithApplicationJson(out);
  }

  @Override
  public GetStudiesByStudyIdResponse getStudiesByStudyId(String studyId) {

    Study study = Study.loadStudy(Resources.getApplicationDataSource(), studyId);

    APIEntity apiEntityTree = entityTreeToAPITree(study.getEntityTree());
    
    APIStudyDetail apiStudyDetail = new APIStudyDetailImpl();
    apiStudyDetail.setId(studyId);
    apiStudyDetail.setRootEntity(apiEntityTree);    
    // TODO: lose or fill in study.setName() prop
    
    StudyIdGetResponse response = new StudyIdGetResponseImpl();
    response.setStudy(apiStudyDetail);
    
    return GetStudiesByStudyIdResponse.respond200WithApplicationJson(response);
  }
  
  private static APIEntity entityTreeToAPITree(TreeNode<Entity> root) {
    return root.mapStructure((entity, mappedChildren) -> {
      APIEntity apiEntity = new APIEntityImpl();
      apiEntity.setDescription(entity.getDescription());
      apiEntity.setName(entity.getName());
      apiEntity.setId(entity.getId());
      apiEntity.setChildren(mappedChildren);
      
      List<APIVariable> apiVariables = new ArrayList<>();
      for (Variable var : entity.getVariables()) 
        apiVariables.add(variableToAPIVariable(var));
      apiEntity.setVariables(apiVariables);

      return apiEntity;
    });
  }
  
  private static APIVariable variableToAPIVariable(Variable var) {
    if (var.getType() == VariableType.DATE) {
      APIDateVariable apiVar = new APIDateVariableImpl();
      setApiVarProps(apiVar, var);
      apiVar.setIsContinuous(var.getIsContinuous() == Variable.IsContinuous.TRUE);    
      return apiVar;
    }
    else if (var.getType() == VariableType.NUMBER) {
      APINumberVariable apiVar = new APINumberVariableImpl();
      setApiVarProps(apiVar, var);
      apiVar.setPrecision(var.getPrecision());
      apiVar.setUnits(var.getUnits());
      apiVar.setIsContinuous(var.getIsContinuous() == Variable.IsContinuous.TRUE);    
      return apiVar;
    }
    else if (var.getType() == VariableType.STRING) {
      APIStringVariable apiVar = new APIStringVariableImpl();
      setApiVarProps(apiVar, var);
      return apiVar;
    }
    else {
      throw new RuntimeException("Impossible enum value");
    }
  }
  
  private static void setApiVarProps(APIVariable apiVar, Variable var) {
    apiVar.setId(var.getId());
    apiVar.setName(var.getName());
    apiVar.setDisplayName(var.getDisplayName());
    apiVar.setParentId(var.getParentId());
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
      
    DataSource datasource = Resources.getApplicationDataSource();
    
    // unpack data from API input to model objects
    List<String> vars = new ArrayList<>();
    vars.add(variableId);  // force into a list for the unpacker
    UnpackedRequest unpacked = unpack(datasource, studyId, entityId, request.getFilters(), vars);

    Variable var = unpacked.entity.getVariable(variableId)
        .orElseThrow(() -> new BadRequestException("Variable ID not found: " + variableId));

    VariableDistributionPostResponseStream streamer = new VariableDistributionPostResponseStream
        (outStream -> StudySubsettingUtils.produceVariableDistribution(datasource, unpacked.study, unpacked.entity, var, unpacked.filters, outStream));

    return PostStudiesEntitiesVariablesDistributionByStudyIdAndEntityIdAndVariableIdResponse.
        respond200WithApplicationJson(streamer);
   }

    @Override
    public PostStudiesEntitiesVariablesCountByStudyIdAndEntityIdAndVariableIdResponse postStudiesEntitiesVariablesCountByStudyIdAndEntityIdAndVariableId(
        String studyId, String entityId, String variableId, VariableCountPostRequest request) {

      DataSource datasource = Resources.getApplicationDataSource();

      // unpack data from API input to model objects
      List<String> vars = new ArrayList<>();
      vars.add(variableId);  // force into a list for the unpacker
      UnpackedRequest unpacked = unpack(datasource, studyId, entityId, request.getFilters(), vars);

      Variable var = unpacked.entity.getVariable(variableId)
          .orElseThrow(() -> new BadRequestException("Variable ID not found: " + variableId));

      Integer count = StudySubsettingUtils.getVariableCount(datasource, unpacked.study, unpacked.entity,
          var, unpacked.filters);

      VariableCountPostResponse response = new VariableCountPostResponseImpl();
      response.setCount(count);

      return  PostStudiesEntitiesVariablesCountByStudyIdAndEntityIdAndVariableIdResponse.respond200WithApplicationJson(response);
    }

    @Override
  public PostStudiesEntitiesTabularByStudyIdAndEntityIdResponse postStudiesEntitiesTabularByStudyIdAndEntityId(String studyId,
      String entityId, EntityTabularPostRequest request) {
    
    DataSource datasource = Resources.getApplicationDataSource();
    
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

    DataSource datasource = Resources.getApplicationDataSource();

    // unpack data from API input to model objects
    List<String> vars = new ArrayList<>();
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
  
  static class UnpackedRequest {
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
    List<Filter> subsetFilters = new ArrayList<>();

    String errPrfx = "A filter references an unfound ";
    
    for (APIFilter apiFilter : filters) {
      
      // validate filter's entity id
      Supplier<BadRequestException> excep = 
          () -> new BadRequestException(errPrfx + "entity ID: " + apiFilter.getEntityId());
      Entity entity = study.getEntity(apiFilter.getEntityId()).orElseThrow(excep);
      
      // validate filter's variable id
      String varId = apiFilter.getVariableId();
      entity.getVariable(varId).orElseThrow(() -> new BadRequestException("Variable '" + varId + "' is not found"));

      Filter newFilter;
      if (apiFilter instanceof APIDateRangeFilter) {
        APIDateRangeFilter f = (APIDateRangeFilter)apiFilter;
        newFilter = new DateRangeFilter(entity, varId,
            convertDate(f.getMin()), convertDate(f.getMax()));
      } else if (apiFilter instanceof APIDateSetFilter) {
        APIDateSetFilter f = (APIDateSetFilter)apiFilter;
        List<LocalDateTime> dateSet = new ArrayList<>();
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
      Entity entity = study.getEntity(outputEntityId).orElseThrow(() -> new RuntimeException("Invalid entityId: " + outputEntityId));
      Variable var = entity.getVariable(varId).orElseThrow(() -> new BadRequestException(errMsg));
      if (var.getEntityId().equals(outputEntityId)) throw new BadRequestException(errMsg);   
    }
  }
  
  static LocalDateTime convertDate(String dateStr) {
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
      return LocalDateTime.parse(dateStr, formatter);
    }
    catch (DateTimeParseException e) {
      throw new BadRequestException("Can't parse date string" + dateStr);
    }
  }
}
