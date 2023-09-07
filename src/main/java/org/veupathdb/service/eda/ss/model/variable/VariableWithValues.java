package org.veupathdb.service.eda.ss.model.variable;

import org.veupathdb.service.eda.ss.Utils;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;

import java.util.List;
import java.util.function.Function;

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
    public final Boolean hasStudyDependentVocabulary;
    public final String variableSpecToImputeZeroesFor;

    public Properties(VariableType type, VariableDataShape dataShape,
                      List<String> vocabulary, Long distinctValuesCount,
                      Boolean isTemporal, Boolean isFeatured,
                      Boolean isMergeKey, Boolean isMultiValued,
                      Boolean imputeZero, Boolean hasStudyDependentVocabulary, String variableSpecToImputeZeroesFor) {
      this.type = type;
      this.dataShape = dataShape;
      this.vocabulary = vocabulary;
      this.distinctValuesCount = distinctValuesCount;
      this.isFeatured = isFeatured;
      this.isTemporal = isTemporal;
      this.isMergeKey = isMergeKey;
      this.isMultiValued = isMultiValued;
      this.imputeZero = imputeZero;
      this.hasStudyDependentVocabulary = hasStudyDependentVocabulary;
      this.variableSpecToImputeZeroesFor = variableSpecToImputeZeroesFor;
    }
  }

  public abstract BinaryProperties getBinaryProperties();

  public abstract BinaryConverter<T> getBinaryConverter();

  public abstract BinaryConverter<String> getStringConverter();

  public abstract T fromString(String s);

  public abstract String valueToString(T val, TabularReportConfig config);

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

  public Boolean hasStudyDependentVocabulary() {
    return _properties.hasStudyDependentVocabulary;
  }

  public String getVariableSpecToImputeZeroesFor() {
    return _properties.variableSpecToImputeZeroesFor;
  }

  /**
   * Method that returns a function which transforms length-padded UTF-8 bytes into a formatted result. For some
   * variables this may be dependent on the tabularReportConfig variable. The default behavior is to remove the padding
   * on the string and wrap it in quotes for multi-val variables.
   *
   * Note that we are returning a function here to avoid branching in the low-level code that gets executed per
   * variable value.
   *
   * @param tabularReportConfig Report configuration for the request
   * @return Function to translate bytes properly for this variable with the given report configuration.
   */
  public Function<byte[], byte[]> getRawUtf8BinaryFormatter(TabularReportConfig tabularReportConfig) {
    return getIsMultiValued() ? Utils::quotePaddedBinary : Utils::trimPaddedBinary;
  }
}
