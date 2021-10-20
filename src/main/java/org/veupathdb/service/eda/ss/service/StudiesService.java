package org.veupathdb.service.eda.ss.service;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.ListBuilder;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.common.client.TabularResponseType;
import org.veupathdb.service.eda.generated.model.APIEntity;
import org.veupathdb.service.eda.generated.model.APIStudyDetail;
import org.veupathdb.service.eda.generated.model.BinSpecWithRange;
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
import org.veupathdb.service.eda.generated.resources.Studies;
import org.veupathdb.service.eda.ss.Resources;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.MetadataCache;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.StudySubsettingUtils;
import org.veupathdb.service.eda.ss.model.distribution.AbstractDistribution;
import org.veupathdb.service.eda.ss.model.distribution.DateBinDistribution;
import org.veupathdb.service.eda.ss.model.distribution.DiscreteDistribution;
import org.veupathdb.service.eda.ss.model.distribution.DistributionResult;
import org.veupathdb.service.eda.ss.model.distribution.FloatingPointBinDistribution;
import org.veupathdb.service.eda.ss.model.distribution.IntegerBinDistribution;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;
import org.veupathdb.service.eda.ss.model.variable.FloatingPointVariable;
import org.veupathdb.service.eda.ss.model.variable.IntegerVariable;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableDataShape;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

public class StudiesService implements Studies {

  private static final Logger LOG = LogManager.getLogger(StudiesService.class);

  @Context
  ContainerRequestContext _request;

  @Override
  public GetStudiesClearMetadataCacheResponse getStudiesClearMetadataCache() {
    MetadataCache.clear();
    return GetStudiesClearMetadataCacheResponse.respond200WithTextPlain(
        "Cache successfully cleared at " + new Date());
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
    APIStudyDetail apiStudyDetail = ApiConversionUtil.getApiStudyDetail(study);
    StudyIdGetResponse response = new StudyIdGetResponseImpl();
    response.setStudy(apiStudyDetail);
    return GetStudiesByStudyIdResponse.respond200WithApplicationJson(response);
  }

  @Override
  public GetStudiesEntitiesByStudyIdAndEntityIdResponse getStudiesEntitiesByStudyIdAndEntityId(String studyId, String entityId) {
    APIStudyDetail apiStudyDetail = ApiConversionUtil.getApiStudyDetail(MetadataCache.getStudy(studyId));
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

  @Override
  public PostStudiesEntitiesVariablesDistributionByStudyIdAndEntityIdAndVariableIdResponse 
  postStudiesEntitiesVariablesDistributionByStudyIdAndEntityIdAndVariableId(
      String studyId, String entityId, String variableId, VariableDistributionPostRequest request) {
    try {

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
    catch (RuntimeException e) {
      LOG.error("Unable to deliver distribution response", e);
      throw e;
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

    return responseType == TabularResponseType.JSON
        ? PostStudiesEntitiesTabularByStudyIdAndEntityIdResponse
            .respond200WithApplicationJson(streamer)
        : PostStudiesEntitiesTabularByStudyIdAndEntityIdResponse
            .respond200WithTextTabSeparatedValues(streamer);
  }

  @Override
  public PostStudiesEntitiesCountByStudyIdAndEntityIdResponse postStudiesEntitiesCountByStudyIdAndEntityId(
      String studyId, String entityId, EntityCountPostRequest rawRequest) {

    DataSource datasource = Resources.getApplicationDataSource();

    // unpack data from API input to model objects
    RequestBundle request = RequestBundle.unpack(datasource, studyId, entityId, rawRequest.getFilters(), Collections.emptyList(), null);

    TreeNode<Entity> prunedEntityTree = StudySubsettingUtils.pruneTree(
        request.getStudy().getEntityTree(), request.getFilters(), request.getTargetEntity());

    long count = StudySubsettingUtils.getEntityCount(
        datasource, prunedEntityTree, request.getTargetEntity(), request.getFilters());

    EntityCountPostResponse response = new EntityCountPostResponseImpl();
    response.setCount(count);

    return  PostStudiesEntitiesCountByStudyIdAndEntityIdResponse.respond200WithApplicationJson(response);
  }

  static DistributionResult processDistributionRequest(DataSource ds, Study study, Entity targetEntity,
                                                       VariableWithValues var, List<Filter> filters,
                                                       ValueSpec valueSpec, Optional<BinSpecWithRange> incomingBinSpec) {
    // inspect requested variable and select appropriate distribution
    AbstractDistribution<?> distribution;
    if (var.getDataShape() == VariableDataShape.CONTINUOUS) {
      distribution = switch(var.getType()) {
        case INTEGER -> new IntegerBinDistribution(ds, study, targetEntity,
            (IntegerVariable)var, filters, valueSpec, incomingBinSpec);
        case NUMBER -> new FloatingPointBinDistribution(ds, study, targetEntity,
            (FloatingPointVariable)var, filters, valueSpec, incomingBinSpec);
        case DATE -> new DateBinDistribution(ds, study, targetEntity,
            (DateVariable)var, filters, valueSpec, incomingBinSpec);
        default -> throw new BadRequestException("Among continuous variables, " +
            "distribution endpoint supports only date, integer, and number types; " +
            "requested variable '" + var.getId() + "' is type " + var.getType());
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

}
