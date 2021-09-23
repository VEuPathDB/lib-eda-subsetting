package org.veupathdb.service.eda.ss.service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.ListBuilder;
import org.gusdb.fgputil.functional.TreeNode;
import org.gusdb.fgputil.json.JsonUtil;
import org.veupathdb.service.eda.common.client.TabularResponseType;
import org.veupathdb.service.eda.generated.model.APIDateVariable;
import org.veupathdb.service.eda.generated.model.APIDateVariableImpl;
import org.veupathdb.service.eda.generated.model.APIEntity;
import org.veupathdb.service.eda.generated.model.APIEntityImpl;
import org.veupathdb.service.eda.generated.model.APIIntegerVariable;
import org.veupathdb.service.eda.generated.model.APIIntegerVariableImpl;
import org.veupathdb.service.eda.generated.model.APILongitudeVariable;
import org.veupathdb.service.eda.generated.model.APILongitudeVariableImpl;
import org.veupathdb.service.eda.generated.model.APINumberVariable;
import org.veupathdb.service.eda.generated.model.APINumberVariableImpl;
import org.veupathdb.service.eda.generated.model.APIStringVariable;
import org.veupathdb.service.eda.generated.model.APIStringVariableImpl;
import org.veupathdb.service.eda.generated.model.APIStudyDetail;
import org.veupathdb.service.eda.generated.model.APIStudyDetailImpl;
import org.veupathdb.service.eda.generated.model.APIVariable;
import org.veupathdb.service.eda.generated.model.APIVariableDataShape;
import org.veupathdb.service.eda.generated.model.APIVariableDisplayType;
import org.veupathdb.service.eda.generated.model.APIVariablesCategory;
import org.veupathdb.service.eda.generated.model.APIVariablesCategoryImpl;
import org.veupathdb.service.eda.generated.model.BinSpecWithRange;
import org.veupathdb.service.eda.generated.model.BinSpecWithRangeImpl;
import org.veupathdb.service.eda.generated.model.EntityCountPostRequest;
import org.veupathdb.service.eda.generated.model.EntityCountPostResponse;
import org.veupathdb.service.eda.generated.model.EntityCountPostResponseImpl;
import org.veupathdb.service.eda.generated.model.EntityIdGetResponse;
import org.veupathdb.service.eda.generated.model.EntityIdGetResponseImpl;
import org.veupathdb.service.eda.generated.model.EntityTabularPostRequest;
import org.veupathdb.service.eda.generated.model.EntityTabularPostResponseStream;
import org.veupathdb.service.eda.generated.model.StudiesGetResponseImpl;
import org.veupathdb.service.eda.generated.model.StudyIdGetResponse;
import org.veupathdb.service.eda.generated.model.StudyIdGetResponseImpl;
import org.veupathdb.service.eda.generated.model.ValueSpec;
import org.veupathdb.service.eda.generated.model.VariableDistributionPostRequest;
import org.veupathdb.service.eda.generated.model.VariableDistributionPostResponse;
import org.veupathdb.service.eda.generated.model.VariableDistributionPostResponseImpl;
import org.veupathdb.service.eda.ss.Resources;
import org.veupathdb.service.eda.ss.model.DateVariable;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.MetadataCache;
import org.veupathdb.service.eda.ss.model.NumberVariable;
import org.veupathdb.service.eda.ss.model.StringVariable;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.StudySubsettingUtils;
import org.veupathdb.service.eda.ss.model.Variable;
import org.veupathdb.service.eda.ss.model.VariableType;
import org.veupathdb.service.eda.ss.model.VariableWithValues;
import org.veupathdb.service.eda.ss.model.distribution.AbstractDistribution;
import org.veupathdb.service.eda.ss.model.distribution.DateBinDistribution;
import org.veupathdb.service.eda.ss.model.distribution.DiscreteDistribution;
import org.veupathdb.service.eda.ss.model.distribution.DistributionResult;
import org.veupathdb.service.eda.ss.model.distribution.NumberBinDistribution;
import org.veupathdb.service.eda.ss.model.filter.Filter;

public class Studies implements org.veupathdb.service.eda.generated.resources.Studies {

  private static final Logger LOG = LogManager.getLogger(Studies.class);

  @Context
  ContainerRequestContext _request;

  @Override
  public GetStudiesClearMetadataCacheResponse getStudiesClearMetadataCache() {
    MetadataCache.clear();
    return GetStudiesClearMetadataCacheResponse.respond202();
  }

  @Override
  public GetStudiesResponse getStudies() {
    var out = new StudiesGetResponseImpl();
    out.setStudies(MetadataCache.getStudyOverviews());
    return GetStudiesResponse.respond200WithApplicationJson(out);
  }

  @Override
  public GetStudiesByStudyIdResponse getStudiesByStudyId(String studyId) {
    Study study = MetadataCache.getStudy(studyId);
    APIStudyDetail apiStudyDetail = getApiStudyDetail(study);
    StudyIdGetResponse response = new StudyIdGetResponseImpl();
    response.setStudy(apiStudyDetail);
    return GetStudiesByStudyIdResponse.respond200WithApplicationJson(response);
  }

  @Override
  public GetStudiesEntitiesByStudyIdAndEntityIdResponse getStudiesEntitiesByStudyIdAndEntityId(String studyId, String entityId) {
    APIStudyDetail apiStudyDetail = getApiStudyDetail(MetadataCache.getStudy(studyId));
    APIEntity entity = findEntityById(apiStudyDetail.getRootEntity(), entityId).orElseThrow(NotFoundException::new);
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

  private Optional<APIEntity> findEntityById(APIEntity entity, String entityId) {
    if (entity.getId().equals(entityId)) {
      return Optional.of(entity);
    }
    for (APIEntity child : entity.getChildren()){
      Optional<APIEntity> foundEntity = findEntityById(child, entityId);
      if (foundEntity.isPresent()) return foundEntity;
    }
    return Optional.empty();
  }

  public static APIStudyDetail getApiStudyDetail(Study study) {
    APIEntity apiEntityTree = entityTreeToAPITree(study.getEntityTree());
    APIStudyDetail apiStudyDetail = new APIStudyDetailImpl();
    apiStudyDetail.setId(study.getStudyId());
    apiStudyDetail.setDatasetId(study.getDatasetId());
    apiStudyDetail.setRootEntity(apiEntityTree);
    return apiStudyDetail;
  }
  
  public static APIEntity entityTreeToAPITree(TreeNode<Entity> root) {
    return root.mapStructure((entity, mappedChildren) -> {
      APIEntity apiEntity = new APIEntityImpl();
      apiEntity.setDescription(entity.getDescription());
      apiEntity.setDisplayName(entity.getDisplayName());
      apiEntity.setDisplayNamePlural(entity.getDisplayNamePlural());
      apiEntity.setId(entity.getId());
      apiEntity.setIdColumnName(entity.getPKColName());
      apiEntity.setChildren(mappedChildren);
      apiEntity.setVariables(entity.getVariables().stream()
          .map(var -> variableToAPIVariable(var))
          .collect(Collectors.toList()));
      return apiEntity;
    });
  }
  
  private static APIVariable variableToAPIVariable(Variable var) {
    if (!var.getHasValues()) {
      APIVariablesCategory apiVar = new APIVariablesCategoryImpl();
      apiVar.setId(var.getId());
      apiVar.setDisplayName(var.getDisplayName());
      apiVar.setDisplayType(APIVariableDisplayType.valueOf(var.getDisplayType().toString()));
      apiVar.setProviderLabel(var.getProviderLabel());
      apiVar.setParentId(var.getParentId());
      apiVar.setDefinition(var.getDefinition());
      apiVar.setDisplayOrder(var.getDisplayOrder());
      return apiVar;
    }
    switch(var.getType()) {
      case DATE:
        DateVariable dateVar = (DateVariable)var;
        APIDateVariable apiDateVar = new APIDateVariableImpl();
        setApiValueVarProps(apiDateVar, var);
        apiDateVar.setBinWidth(dateVar.getBinSize());
        apiDateVar.setBinWidthOverride(null);
        apiDateVar.setBinUnits(dateVar.getBinUnits());
        apiDateVar.setDisplayRangeMin(dateVar.getDisplayRangeMin());
        apiDateVar.setDisplayRangeMax(dateVar.getDisplayRangeMax());
        apiDateVar.setRangeMin(dateVar.getRangeMin());
        apiDateVar.setRangeMax(dateVar.getRangeMax());
        apiDateVar.setVocabulary(dateVar.getVocabulary());
        apiDateVar.setDistinctValuesCount(dateVar.getDistinctValuesCount());
        apiDateVar.setIsFeatured(dateVar.getIsFeatured());
        apiDateVar.setIsMergeKey(dateVar.getIsMergeKey());
        apiDateVar.setIsMultiValued(dateVar.getIsMultiValued());
        apiDateVar.setIsTemporal(dateVar.getIsTemporal());
        return apiDateVar;
      case INTEGER:
        NumberVariable intVar = (NumberVariable)var;
        APIIntegerVariable apiIntVar = new APIIntegerVariableImpl();
        setApiValueVarProps(apiIntVar, var);
        apiIntVar.setUnits(intVar.getUnits());
        apiIntVar.setBinWidth(intVar.getBinWidth());
        apiIntVar.setBinWidthOverride(intVar.getBinWidthOverride());
        apiIntVar.setDisplayRangeMin(intVar.getDisplayRangeMin());
        apiIntVar.setDisplayRangeMax(intVar.getDisplayRangeMax());
        apiIntVar.setRangeMin(intVar.getRangeMin());
        apiIntVar.setRangeMax(intVar.getRangeMax());
        apiIntVar.setDistinctValuesCount(intVar.getDistinctValuesCount());
        apiIntVar.setIsFeatured(intVar.getIsFeatured());
        apiIntVar.setIsMergeKey(intVar.getIsMergeKey());
        apiIntVar.setIsMultiValued(intVar.getIsMultiValued());
        apiIntVar.setIsTemporal(intVar.getIsTemporal());
        apiIntVar.setVocabulary(intVar.getVocabulary());
        return apiIntVar;
      case NUMBER:
        NumberVariable numVar = (NumberVariable)var;
        APINumberVariable apiNumVar = new APINumberVariableImpl();
        setApiValueVarProps(apiNumVar, var);
        apiNumVar.setUnits(numVar.getUnits());
        apiNumVar.setPrecision(numVar.getPrecision());
        apiNumVar.setBinWidth(numVar.getBinWidth());
        apiNumVar.setBinWidthOverride(numVar.getBinWidthOverride());
        apiNumVar.setDisplayRangeMin(numVar.getDisplayRangeMin());
        apiNumVar.setDisplayRangeMax(numVar.getDisplayRangeMax());
        apiNumVar.setRangeMin(numVar.getRangeMin());
        apiNumVar.setRangeMax(numVar.getRangeMax());
        apiNumVar.setDistinctValuesCount(numVar.getDistinctValuesCount());
        apiNumVar.setIsFeatured(numVar.getIsFeatured());
        apiNumVar.setIsMergeKey(numVar.getIsMergeKey());
        apiNumVar.setIsMultiValued(numVar.getIsMultiValued());
        apiNumVar.setIsTemporal(numVar.getIsTemporal());
        apiNumVar.setVocabulary(numVar.getVocabulary());
        return apiNumVar;
      case STRING:
        StringVariable strVar = (StringVariable)var;
        APIStringVariable apiStrVar = new APIStringVariableImpl();
        setApiValueVarProps(apiStrVar, var);
        apiStrVar.setDistinctValuesCount(strVar.getDistinctValuesCount());
        apiStrVar.setIsFeatured(strVar.getIsFeatured());
        apiStrVar.setIsMergeKey(strVar.getIsMergeKey());
        apiStrVar.setIsMultiValued(strVar.getIsMultiValued());
        apiStrVar.setIsTemporal(strVar.getIsTemporal());
        apiStrVar.setVocabulary(strVar.getVocabulary());
        return apiStrVar;
      case LONGITUDE:
        NumberVariable longVar = (NumberVariable)var;
        APILongitudeVariable apiLongVar = new APILongitudeVariableImpl();
        setApiValueVarProps(apiLongVar, var);
        apiLongVar.setDistinctValuesCount(longVar.getDistinctValuesCount());
        apiLongVar.setIsFeatured(false);
        apiLongVar.setIsMergeKey(false);
        apiLongVar.setIsMultiValued(false);
        apiLongVar.setIsTemporal(false);
        return apiLongVar;
      default:
        throw new RuntimeException("Invalid variable type " + var.getType());
    }
  }
  
  private static void setApiVarProps(APIVariable apiVar, Variable var) {
    apiVar.setId(var.getId());
    apiVar.setDisplayName(var.getDisplayName());
    apiVar.setDisplayType(APIVariableDisplayType.valueOf(var.getDisplayType().toString()));
    apiVar.setProviderLabel(var.getProviderLabel());
    apiVar.setParentId(var.getParentId());
    apiVar.setDefinition(var.getDefinition());
    apiVar.setDisplayOrder(var.getDisplayOrder());
  }
  
  private static void setApiValueVarProps(APIVariable apiVar, Variable var) {
    setApiVarProps(apiVar, var);
    apiVar.setDataShape(APIVariableDataShape.valueOf(var.getDataShape().toString()));
  }

  @Override
  public PostStudiesEntitiesVariablesDistributionByStudyIdAndEntityIdAndVariableIdResponse 
  postStudiesEntitiesVariablesDistributionByStudyIdAndEntityIdAndVariableId(
      String studyId, String entityId, String variableId, VariableDistributionPostRequest request) {

    // unpack data from API input to model objects
    DataSource ds = Resources.getApplicationDataSource();
    RequestBundle req = RequestBundle.unpack(ds, studyId, entityId, request.getFilters(), ListBuilder.asList(variableId), null);
    VariableWithValues var = getRequestedVariable(req);

    DistributionResult result = processDistributionRequest(ds, req.getStudy(),
        req.getTargetEntity(), var, req.getFilters(), request.getValueSpec(),
        Optional.ofNullable(request.getBinSpec()));

    VariableDistributionPostResponse response = new VariableDistributionPostResponseImpl();
    response.setHistogram(result.getHistogramData());
    response.setStatistics(result.getStatistics());

    return PostStudiesEntitiesVariablesDistributionByStudyIdAndEntityIdAndVariableIdResponse.
        respond200WithApplicationJson(response);
  }

  static DistributionResult processDistributionRequest(DataSource ds, Study study, Entity targetEntity,
      VariableWithValues var, List<Filter> filters, ValueSpec valueSpec, Optional<BinSpecWithRange> incomingBinSpec) {
    // inspect requested variable and select appropriate distribution
    AbstractDistribution<?> distribution;
    if (var.getDataShape() == Variable.VariableDataShape.CONTINUOUS) {
      BinSpecWithRange binSpec = validateVarAndBinSpec(var, incomingBinSpec);
      LOG.debug("Found var of type: " + var.getType() + ", will use bin spec: " + JsonUtil.serializeObject(binSpec));
      distribution = switch(var.getType()) {
        case INTEGER, NUMBER -> new NumberBinDistribution(ds, study, targetEntity,
            (NumberVariable)var, filters, valueSpec, binSpec);
        case DATE -> new DateBinDistribution(ds, study, targetEntity,
            (DateVariable)var, filters, valueSpec, binSpec);
        default -> throw new BadRequestException(
            "Requested variable '" + var.getId() + "' must be a date or number, but is " + var.getType());
      };
    }
    else {
      if (incomingBinSpec.isPresent()) {
        throw new BadRequestException("Bin spec is allowed/required only for continuous variables.");
      }
      distribution = new DiscreteDistribution(ds, study, targetEntity, var, filters, valueSpec);
    }

    return distribution.generateDistribution();
  }

  private VariableWithValues getRequestedVariable(RequestBundle req) {
    if (req.getRequestedVariables().isEmpty()) {
      throw new RuntimeException("No requested variables (empty URL segment?)");
    }
    Variable var = req.getRequestedVariables().get(0);
    if (!(var instanceof VariableWithValues)) {
      throw new BadRequestException("Distribution endpoint can only be called with a variable that has values.");
    }
    return (VariableWithValues)var;
  }

  private static BinSpecWithRange validateVarAndBinSpec(Variable var, Optional<BinSpecWithRange> submittedBinSpec) {
    // bin spec is needed; type determined by var type
    switch(var.getType()) {
      case STRING:
      case LONGITUDE:
        throw new BadRequestException("Among continuous variables, " +
            "distribution endpoint supports only date, integer, and number types.");
      case DATE:
        DateVariable dateVar = (DateVariable)var;
        if (submittedBinSpec.isEmpty()) {
          // use defaults
          BinSpecWithRange binSpec = new BinSpecWithRangeImpl();
          binSpec.setDisplayRangeMin(dateVar.getRangeMin());
          binSpec.setDisplayRangeMax(dateVar.getRangeMax());
          binSpec.setBinUnits(dateVar.getDefaultBinUnits());
          binSpec.setBinWidth(dateVar.getBinSize());
          return binSpec;
        }
        else {
          BinSpecWithRange binSpec = submittedBinSpec.get();
          if (binSpec.getBinUnits() == null) {
            throw new BadRequestException("binUnits is required for date variable distributions");
          }
          if (binSpec.getBinWidth().intValue() <= 0) {
            throw new BadRequestException("binWidth must be a positive integer for date variable distributions");
          }
          return binSpec;
        }
      case NUMBER:
      case INTEGER:
        NumberVariable numberVar = (NumberVariable)var;
        if (submittedBinSpec.isEmpty()) {
          // use defaults
          BinSpecWithRange binSpec = new BinSpecWithRangeImpl();
          binSpec.setDisplayRangeMin(numberVar.getDisplayRangeMin());
          binSpec.setDisplayRangeMax(numberVar.getDisplayRangeMax());
          binSpec.setBinWidth(numberVar.getDefaultBinWidth());
          return binSpec;
        }
        else {
          BinSpecWithRange binSpec = submittedBinSpec.get();
          if (binSpec.getBinUnits() != null || binSpec.getBinWidth() == null) {
            throw new BadRequestException("For number variables, only binWidth should be submitted (not binUnits).");
          }
          if (binSpec.getBinWidth().doubleValue() <= 0) {
            throw new BadRequestException("binWidth must be a positive number for number variable distributions");
          }
          return binSpec;
        }
      default:
        throw new RuntimeException("Must add case statement here for type: " + var.getType());
    }
  }

  @Override
  public PostStudiesEntitiesTabularByStudyIdAndEntityIdResponse postStudiesEntitiesTabularByStudyIdAndEntityId(String studyId,
      String entityId, EntityTabularPostRequest requestBody) {

    DataSource datasource = Resources.getApplicationDataSource();

    RequestBundle request = RequestBundle.unpack(datasource, studyId, entityId, requestBody.getFilters(), requestBody.getOutputVariableIds(), requestBody.getReportConfig());

    TabularResponseType responseType = TabularResponseType.fromAcceptHeader(_request);

    EntityTabularPostResponseStream streamer = new EntityTabularPostResponseStream
        (outStream -> StudySubsettingUtils.produceTabularSubset(datasource, request.getStudy(),
            request.getTargetEntity(), request.getRequestedVariables(), request.getFilters(),
            request.getReportConfig(), responseType.getFormatter(), outStream));

    return switch(responseType) {
      case JSON -> PostStudiesEntitiesTabularByStudyIdAndEntityIdResponse
          .respond200WithApplicationJson(streamer);
      default -> PostStudiesEntitiesTabularByStudyIdAndEntityIdResponse
        .respond200WithTextTabSeparatedValues(streamer);
    };
  }

  @Override
  public PostStudiesEntitiesCountByStudyIdAndEntityIdResponse postStudiesEntitiesCountByStudyIdAndEntityId(
      String studyId, String entityId, EntityCountPostRequest rawRequest) {

    DataSource datasource = Resources.getApplicationDataSource();

    // unpack data from API input to model objects
	RequestBundle request = RequestBundle.unpack(datasource, studyId, entityId, rawRequest.getFilters(), Collections.emptyList(), null);

    TreeNode<Entity> prunedEntityTree = StudySubsettingUtils.pruneTree(
        request.getStudy().getEntityTree(), request.getFilters(), request.getTargetEntity());

    int count = StudySubsettingUtils.getEntityCount(
        datasource, prunedEntityTree, request.getTargetEntity(), request.getFilters());

    EntityCountPostResponse response = new EntityCountPostResponseImpl();
    response.setCount(count);

    return  PostStudiesEntitiesCountByStudyIdAndEntityIdResponse.respond200WithApplicationJson(response);
  }

}
