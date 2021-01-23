package org.veupathdb.service.edass.service;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.util.Optional;
import org.gusdb.fgputil.Tuples.TwoTuple;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.edass.Resources;
import org.veupathdb.service.edass.generated.model.*;
import org.veupathdb.service.edass.model.*;
import org.veupathdb.service.edass.model.Variable.VariableType;

import javax.sql.DataSource;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.gusdb.fgputil.functional.Functions.cSwallow;

public class Studies implements org.veupathdb.service.edass.generated.resources.Studies {
  Map<String, APIStudyOverview> apiStudyOverviews;  // cache the overviews

  @Override
  public GetStudiesResponse getStudies() {
    var out = new StudiesGetResponseImpl();
    out.setStudies(getStudyOverviews(Resources.getApplicationDataSource()));
    return GetStudiesResponse.respond200WithApplicationJson(out);
  }

  private List<APIStudyOverview> getStudyOverviews(DataSource datasource) {
    if (apiStudyOverviews == null) {
      List<Study.StudyOverview> overviews = Study.getStudyOverviews(datasource);
      apiStudyOverviews = new LinkedHashMap<>();
      for (Study.StudyOverview overview : overviews) {
        APIStudyOverview study = new APIStudyOverviewImpl();
        study.setId(overview.getId());
        apiStudyOverviews.put(study.getId(), study);
      }
    }
    return new ArrayList<>( apiStudyOverviews.values() );
  }

  @Override
  public GetStudiesByStudyIdResponse getStudiesByStudyId(String studyId) {

    APIStudyDetail apiStudyDetail = getApiStudyDetail(studyId);
    StudyIdGetResponse response = new StudyIdGetResponseImpl();
    response.setStudy(apiStudyDetail);
    
    return GetStudiesByStudyIdResponse.respond200WithApplicationJson(response);
  }

  @Override
  public GetStudiesEntitiesByStudyIdAndEntityIdResponse getStudiesEntitiesByStudyIdAndEntityId(String studyId, String entityId) {
    APIStudyDetail apiStudyDetail = getApiStudyDetail(studyId);
    APIEntity entity = findEntityById(apiStudyDetail.getRootEntity(), entityId).orElseThrow(() -> new NotFoundException());
    EntityIdGetResponse response = new EntityIdGetResponseImpl();
    // copy properties of found entity, skipping children
    response.setId(entity.getId());
    response.setDescription(entity.getDescription());
    response.setDisplayName(entity.getDisplayName());
    response.setDisplayNamePlural(entity.getDisplayNamePlural());
    response.setVariables(entity.getVariables());
    for (Map.Entry<String,Object> prop : entity.getAdditionalProperties().entrySet()) {
      response.setAdditionalProperties(prop.getKey(), prop.getValue());
    }
    return GetStudiesEntitiesByStudyIdAndEntityIdResponse.respond200WithApplicationJson(response);
  }

  private static Optional<APIEntity> findEntityById(APIEntity entity, String entityId) {
    if (entity.getId().equals(entityId)) {
      return Optional.of(entity);
    }
    for (APIEntity child : entity.getChildren()){
      Optional<APIEntity> foundEntity = findEntityById(child, entityId);
      if (foundEntity.isPresent()) return foundEntity;
    }
    return Optional.empty();
  }

  public static APIStudyDetail getApiStudyDetail(String studyId) {
    Study study = Study.loadStudy(Resources.getApplicationDataSource(), studyId);

    APIEntity apiEntityTree = entityTreeToAPITree(study.getEntityTree());

    APIStudyDetail apiStudyDetail = new APIStudyDetailImpl();
    apiStudyDetail.setId(studyId);
    apiStudyDetail.setRootEntity(apiEntityTree);
    // TODO: lose or fill in study.setName() prop
    return apiStudyDetail;
  }
  
  public static APIEntity entityTreeToAPITree(TreeNode<Entity> root) {
    return root.mapStructure((entity, mappedChildren) -> {
      APIEntity apiEntity = new APIEntityImpl();
      apiEntity.setDescription(entity.getDescription());
      apiEntity.setDisplayName(entity.getDisplayName());
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
    if (!var.getHasValues()) {
      APIVariablesCategory apiVar = new APIVariablesCategoryImpl();
      setApiVarProps(apiVar, var);
      return apiVar;
    }
    if (var.getType() == VariableType.DATE) {
      APIDateVariable apiVar = new APIDateVariableImpl();
      setApiVarProps(apiVar, var);
      apiVar.setDataShape(APIVariableDataShape.valueOf(var.getDataShape().toString()));
      apiVar.setDisplayType(APIVariableDisplayType.valueOf(var.getDisplayType().toString()));
      return apiVar;
    }
    else if (var.getType() == VariableType.NUMBER) {
      APINumberVariable apiVar = new APINumberVariableImpl();
      setApiVarProps(apiVar, var);
      apiVar.setPrecision(var.getPrecision());
      apiVar.setUnits(var.getUnits());
      apiVar.setDataShape(APIVariableDataShape.valueOf(var.getDataShape().toString()));
      apiVar.setDisplayType(APIVariableDisplayType.valueOf(var.getDisplayType().toString()));
      return apiVar;
    }
    else if (var.getType() == VariableType.STRING) {
      APIStringVariable apiVar = new APIStringVariableImpl();
      apiVar.setDataShape(APIVariableDataShape.valueOf(var.getDataShape().toString()));
      apiVar.setDisplayType(APIVariableDisplayType.valueOf(var.getDisplayType().toString()));
      setApiVarProps(apiVar, var);
      return apiVar;
    }
    else {
      throw new RuntimeException("Impossible enum value");
    }
  }
  
  private static void setApiVarProps(APIVariable apiVar, Variable var) {
    apiVar.setId(var.getId());
    apiVar.setDisplayName(var.getDisplayName());
    apiVar.setProviderLabel(var.getProviderLabel());
    apiVar.setParentId(var.getParentId());
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

    Variable var = unpacked.getTargetEntity().getVariable(variableId)
        .orElseThrow(() -> new NotFoundException("Variable ID not found: " + variableId));

    VariableDistributionPostResponseStream streamer = getDistributionResponseStreamer(
        datasource, unpacked.getStudy(), unpacked.getTargetEntity(), unpacked.getFilters(), var);

    return PostStudiesEntitiesVariablesDistributionByStudyIdAndEntityIdAndVariableIdResponse.
        respond200WithApplicationJson(streamer);
  }

  public static VariableDistributionPostResponseStream getDistributionResponseStreamer(
      DataSource dataSource,
      Study study,
      Entity targetEntity,
      List<Filter> filters,
      Variable variable
  ) {
    // get entity tree pruned to those entities of current interest
    TreeNode<Entity> prunedEntityTree = StudySubsettingUtils.pruneTree(
        study.getEntityTree(), filters, targetEntity);

    // get variable count (may do in parallel later)
    int variableCount = StudySubsettingUtils.getVariableCount(
        dataSource, prunedEntityTree, targetEntity, variable, filters);

    return new VariableDistributionPostResponseStream(outStream -> {
      BufferedOutputStream bufferedOutput = new BufferedOutputStream(outStream);
      try(
        // create a stream of distribution tuples converted from a database result
        Stream<TwoTuple<String, Integer>> distributionStream =
            StudySubsettingUtils.produceVariableDistribution(
              dataSource, prunedEntityTree, targetEntity, variable, filters);
        // create a JSON generator around the output stream
        JsonGenerator json = new JsonFactory().createGenerator(bufferedOutput, JsonEncoding.UTF8)
      ) {
        // write the output
        json.writeStartObject();
        json.writeNumberField("entitiesCount", variableCount);
        json.writeFieldName("distribution");
        json.writeStartObject();
        distributionStream.forEach(cSwallow(row -> json.writeNumberField(row.getKey(), row.getValue())));
        json.writeEndObject();
        json.writeEndObject();
        json.flush();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public PostStudiesEntitiesTabularByStudyIdAndEntityIdResponse postStudiesEntitiesTabularByStudyIdAndEntityId(String studyId,
      String entityId, EntityTabularPostRequest requestBody) {
    
    DataSource datasource = Resources.getApplicationDataSource();
    
    UnpackedRequest request = unpack(datasource, studyId, entityId, requestBody.getFilters(), requestBody.getOutputVariableIds());
    
    EntityTabularPostResponseStream streamer = new EntityTabularPostResponseStream
        (outStream -> StudySubsettingUtils.produceTabularSubset(datasource, request.getStudy(),
            request.getTargetEntity(), request.getRequestedVariables(), request.getFilters(), outStream));

    return PostStudiesEntitiesTabularByStudyIdAndEntityIdResponse
        .respond200WithTextPlain(streamer);
  }

  @Override
  public PostStudiesEntitiesCountByStudyIdAndEntityIdResponse postStudiesEntitiesCountByStudyIdAndEntityId(
      String studyId, String entityId, EntityCountPostRequest rawRequest) {

    DataSource datasource = Resources.getApplicationDataSource();

    // unpack data from API input to model objects
    List<String> vars = new ArrayList<>();
    UnpackedRequest request = unpack(datasource, studyId, entityId, rawRequest.getFilters(), vars);

    TreeNode<Entity> prunedEntityTree = StudySubsettingUtils.pruneTree(
        request.getStudy().getEntityTree(), request.getFilters(), request.getTargetEntity());

    int count = StudySubsettingUtils.getEntityCount(
        datasource, prunedEntityTree, request.getTargetEntity(), request.getFilters());

    EntityCountPostResponse response = new EntityCountPostResponseImpl();
    response.setCount(count);

    return  PostStudiesEntitiesCountByStudyIdAndEntityIdResponse.respond200WithApplicationJson(response);
  }
  
  private UnpackedRequest unpack(DataSource datasource, String studyId, String entityId, List<APIFilter> apiFilters, List<String> variableIds) {
    String studIdStr = "Study ID " + studyId;
    if (!validateStudyId(datasource, studyId))
      throw new NotFoundException(studIdStr + " is not found.");
   
    Study study = Study.loadStudy(datasource, studyId);
    Entity entity = study.getEntity(entityId).orElseThrow(() -> new NotFoundException("In " + studIdStr + " Entity ID not found: " + entityId));
    
    List<Variable> variables = getEntityVariables(entity, variableIds);

    List<Filter> filters = constructFiltersFromAPIFilters(study, apiFilters);
  
    return new UnpackedRequest(study, entity, variables, filters);
  }
  /*
   * return true if valid study id
   */
  public boolean validateStudyId(DataSource datasource, String studyId) {
    return getStudyOverviews(datasource).stream()
            .anyMatch(study -> study.getId().equals(studyId));
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

  static List<Variable> getEntityVariables(Entity entity, List<String> variableIds) {

    List<Variable> variables = new ArrayList<>();
    
    for (String varId : variableIds) {
      String errMsg = "Variable '" + varId + "' is not found for entity with ID: '" + entity.getId() + "'";
      variables.add(entity.getVariable(varId).orElseThrow(() -> new BadRequestException(errMsg)));
    }
    return variables;
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

  static class UnpackedRequest {

    private final Study _study;
    private final List<Filter> _filters;
    private final Entity _targetEntity;
    private final List<Variable> _requestedVariables;

    UnpackedRequest(Study study, Entity targetEntity, List<Variable> requestedVariables, List<Filter> filters) {
      _study = study;
      _targetEntity = targetEntity;
      _filters = filters;
      _requestedVariables = requestedVariables;
    }

    public Study getStudy() {
      return _study;
    }

    public List<Filter> getFilters() {
      return _filters;
    }

    public Entity getTargetEntity() {
      return _targetEntity;
    }

    public List<Variable> getRequestedVariables() {
      return _requestedVariables;
    }
  }
}
