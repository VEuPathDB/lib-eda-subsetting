package org.veupathdb.service.eda.ss.service;

import java.util.stream.Collectors;
import org.gusdb.fgputil.functional.Functions;
import org.gusdb.fgputil.functional.TreeNode;
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
import org.veupathdb.service.eda.generated.model.APIVariableDisplayType;
import org.veupathdb.service.eda.generated.model.APIVariableWithValues;
import org.veupathdb.service.eda.generated.model.APIVariablesCategoryImpl;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.distribution.DistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;
import org.veupathdb.service.eda.ss.model.variable.FloatingPointVariable;
import org.veupathdb.service.eda.ss.model.variable.IntegerVariable;
import org.veupathdb.service.eda.ss.model.variable.LongitudeVariable;
import org.veupathdb.service.eda.ss.model.variable.StringVariable;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

public class ApiConversionUtil {

  /** converts model study object to API study object */
  public static APIStudyDetail getApiStudyDetail(Study study) {
    APIEntity apiEntityTree = entityTreeToAPITree(study.getEntityTree());
    APIStudyDetail apiStudyDetail = new APIStudyDetailImpl();
    apiStudyDetail.setId(study.getStudyId());
    apiStudyDetail.setDatasetId(study.getDatasetId());
    apiStudyDetail.setRootEntity(apiEntityTree);
    return apiStudyDetail;
  }

  /** converts model entity object to API entity object */
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

  /** converts model variable object to API variable object */
  private static APIVariable variableToAPIVariable(Variable var) {
    APIVariable apiVar = var.hasValues()
        ? getValuesVar((VariableWithValues)var)
        : new APIVariablesCategoryImpl();
    // set props common to all variables
    apiVar.setId(var.getId());
    apiVar.setDisplayName(var.getDisplayName());
    apiVar.setDisplayType(APIVariableDisplayType.valueOf(var.getDisplayType().toString()));
    apiVar.setProviderLabel(var.getProviderLabel());
    apiVar.setParentId(var.getParentId());
    apiVar.setDefinition(var.getDefinition());
    apiVar.setDisplayOrder(var.getDisplayOrder());
    return apiVar;
  }

  /** instantiates API values var and sets values-var-specific props */
  private static APIVariableWithValues getValuesVar(VariableWithValues var) {
    APIVariableWithValues apiVar = switch(var.getType()) {
      case DATE -> getDateVar((DateVariable)var);
      case INTEGER -> getIntegerVar((IntegerVariable)var);
      case NUMBER -> getFloatVar((FloatingPointVariable)var);
      case LONGITUDE -> getLongitudeVar((LongitudeVariable)var);
      case STRING -> getStringVar((StringVariable)var);
      default -> Functions.doThrow(() -> new RuntimeException("Invalid variable type " + var.getType()));
    };
    // set props common to all variables with values
    apiVar.setDataShape(var.getDataShape().toApiShape());
    apiVar.setVocabulary(var.getVocabulary());
    apiVar.setDistinctValuesCount(var.getDistinctValuesCount());
    apiVar.setIsFeatured(var.getIsFeatured());
    apiVar.setIsMergeKey(var.getIsMergeKey());
    apiVar.setIsMultiValued(var.getIsMultiValued());
    apiVar.setIsTemporal(var.getIsTemporal());
    return apiVar;
  }

  /** instantiates API date var and sets date-specific props */
  private static APIDateVariable getDateVar(DateVariable var) {
    APIDateVariable apiVar = new APIDateVariableImpl();
    apiVar.setBinWidth(var.getBinSize());
    apiVar.setBinWidthOverride(null);
    apiVar.setBinUnits(var.getBinUnits());
    apiVar.setDisplayRangeMin(var.getDisplayRangeMin());
    apiVar.setDisplayRangeMax(var.getDisplayRangeMax());
    apiVar.setRangeMin(var.getRangeMin());
    apiVar.setRangeMax(var.getRangeMax());
    return apiVar;
  }

  private static APIIntegerVariable getIntegerVar(IntegerVariable var) {
    DistributionConfig<Long> bins = var.getDistributionConfig();
    APIIntegerVariable apiVar = new APIIntegerVariableImpl();
    apiVar.setUnits(var.getUnits());
    apiVar.setBinWidth(bins.getBinWidth());
    apiVar.setBinWidthOverride(bins.getBinWidthOverride());
    apiVar.setDisplayRangeMin(bins.getDisplayRangeMin());
    apiVar.setDisplayRangeMax(bins.getDisplayRangeMax());
    apiVar.setRangeMin(bins.getRangeMin());
    apiVar.setRangeMax(bins.getRangeMax());
    return apiVar;
  }

  private static APINumberVariable getFloatVar(FloatingPointVariable var) {
    DistributionConfig<Double> bins = var.getDistributionConfig();
    APINumberVariable apiVar = new APINumberVariableImpl();
    apiVar.setUnits(var.getUnits());
    apiVar.setPrecision(var.getPrecision());
    apiVar.setBinWidth(bins.getBinWidth());
    apiVar.setBinWidthOverride(bins.getBinWidthOverride());
    apiVar.setDisplayRangeMin(bins.getDisplayRangeMin());
    apiVar.setDisplayRangeMax(bins.getDisplayRangeMax());
    apiVar.setRangeMin(bins.getRangeMin());
    apiVar.setRangeMax(bins.getRangeMax());
    return apiVar;
  }

  private static APIStringVariable getStringVar(StringVariable var) {
    APIStringVariable apiStrVar = new APIStringVariableImpl();
    // currently no string-specific extra properties
    return apiStrVar;
  }

  private static APILongitudeVariable getLongitudeVar(LongitudeVariable var) {
    APILongitudeVariable apiVar = new APILongitudeVariableImpl();
    apiVar.setPrecision(var.getPrecision());
    return apiVar;
  }

}
