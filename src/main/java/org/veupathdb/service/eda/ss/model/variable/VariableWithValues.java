package org.veupathdb.service.eda.ss.model.variable;

import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;

import java.util.List;

public abstract class VariableWithValues<T> extends Variable {

  public static class Properties {

    public final VariableType type;
    public final VariableDataShape dataShape;
    public final List<String> vocabulary;
    public final Long distinctValuesCount;
    public final Boolean isTemporal;
    public final Boolean isFeatured;
    public final Boolean isMergeKey;
    public final Boolean isMultiValued;
    public final Boolean imputeZero;

    public Properties(VariableType type, VariableDataShape dataShape,
                      List<String> vocabulary, Long distinctValuesCount,
                      Boolean isTemporal, Boolean isFeatured,
                      Boolean isMergeKey, Boolean isMultiValued,
                      Boolean imputeZero) {
      this.type = type;
      this.dataShape = dataShape;
      this.vocabulary = vocabulary;
      this.distinctValuesCount = distinctValuesCount;
      this.isFeatured = isFeatured;
      this.isTemporal = isTemporal;
      this.isMergeKey = isMergeKey;
      this.isMultiValued = isMultiValued;
      this.imputeZero = imputeZero;
    }
  }

  public abstract BinaryConverter<T> getBinaryConverter();

  public abstract T fromString(String s);

  public abstract String valueToString(T val, TabularReportConfig config);

  public abstract String valueToJsonText(T val, TabularReportConfig config);

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

  public Long getDistinctValuesCount() {
    return _properties.distinctValuesCount;
  }

  public Boolean getIsMultiValued() {
    return _properties.isMultiValued;
  }

  public Boolean getImputeZero() {
    return _properties.imputeZero;
  }

  public VariableDataShape getDataShape() {
    return _properties.dataShape;
  }

  public VariableType getType() {
    return _properties.type;
  }

  protected String quote(String s) {
    return "\"" + s + "\"";
  }
}
