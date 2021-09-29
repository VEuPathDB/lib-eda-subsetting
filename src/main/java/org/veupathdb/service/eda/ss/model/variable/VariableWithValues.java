package org.veupathdb.service.eda.ss.model.variable;

import java.util.List;
import org.veupathdb.service.eda.ss.model.Entity;

public abstract class VariableWithValues extends Variable {

  public static class Properties {

    public final VariableType type;
    public final VariableDataShape dataShape;
    public final List<String> vocabulary;
    public final Number distinctValuesCount;
    public final Boolean isTemporal;
    public final Boolean isFeatured;
    public final Boolean isMergeKey;
    public final Boolean isMultiValued;

    public Properties(VariableType type, VariableDataShape dataShape,
                      List<String> vocabulary, Number distinctValuesCount,
                      Boolean isTemporal, Boolean isFeatured,
                      Boolean isMergeKey, Boolean isMultiValued) {
      this.type = type;
      this.dataShape = dataShape;
      this.vocabulary = vocabulary;
      this.distinctValuesCount = distinctValuesCount;
      this.isFeatured = isFeatured;
      this.isTemporal = isTemporal;
      this.isMergeKey = isMergeKey;
      this.isMultiValued = isMultiValued;
    }
  }

  private final Properties _properties;

  public VariableWithValues(Variable.Properties varProperties, Properties properties) {
    super(varProperties);
    _properties = properties;
  }

  protected void validateType(VariableType type) {
    if (getType() != type) {
      throw new RuntimeException("Cannot instantiate class " + getClass().getSimpleName() + " with type " + type);
    }
  }

  @Override
  public boolean hasValues() {
    return true;
  }

  public List<String> getVocabulary() {
    return _properties.vocabulary;
  }

  public Boolean getIsTemporal() {
    return _properties.isTemporal;
  }

  public Boolean getIsFeatured() {
    return _properties.isFeatured;
  }

  public Boolean getIsMergeKey() {
    return _properties.isMergeKey;
  }

  public Number getDistinctValuesCount() {
    return _properties.distinctValuesCount;
  }

  public Boolean getIsMultiValued() {
    return _properties.isMultiValued;
  }

  public VariableDataShape getDataShape() {
    return _properties.dataShape;
  }

  public VariableType getType() {
    return _properties.type;
  }

}
