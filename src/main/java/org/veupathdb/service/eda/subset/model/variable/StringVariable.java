package org.veupathdb.service.eda.subset.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.gusdb.fgputil.FormatUtil;
import org.veupathdb.service.eda.subset.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.ByteArrayConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.StringValueConverter;

public class StringVariable extends VariableWithValues<byte[]> {

  private final StringBinaryProperties binaryProperties;

  public StringVariable(Variable.Properties varProperties,
                        VariableWithValues.Properties valueProperties,
                        StringBinaryProperties binaryProperties) {
    super(varProperties, valueProperties);
    validateType(VariableType.STRING);
    this.binaryProperties = binaryProperties;
  }
  
  // static version for use when we don't have an instance
  public static BinaryConverter<byte[]> getGenericBinaryConverter(BinaryProperties binaryProperties) {
    return new ByteArrayConverter(((StringBinaryProperties) binaryProperties).maxLength);
  }

  @Override
  public BinaryProperties getBinaryProperties() {
    return binaryProperties;
  }

  @Override
  public BinaryConverter<byte[]> getBinaryConverter() {
    if (binaryProperties == null) {
      throw new IllegalStateException("No metadata file was found to parse binary properties from for variable " + getId());
    }
    return getGenericBinaryConverter(binaryProperties);
  }

  @Override
  public BinaryConverter<String> getStringConverter() {
    if (binaryProperties == null) {
      throw new IllegalStateException("No metadata file was found to parse binary properties from for variable " + getId());
    }
    return new StringValueConverter(binaryProperties.getMaxLength());
  }

  @Override
  public byte[] fromString(String s) {
    return FormatUtil.stringToPaddedBinary(s, binaryProperties.maxLength);
  }

  @Override
  public String valueToString(byte[] val, TabularReportConfig reportConfig) {
    return FormatUtil.paddedBinaryToString(val);
  }
  
  public static StringVariable assertType(Variable variable) {
    if (variable instanceof StringVariable) return (StringVariable)variable;
    throw new BadRequestException("Variable " + variable.getId() +
        " of entity " + variable.getEntityId() + " is not a string variable.");
  }

  public static class StringBinaryProperties extends BinaryProperties {
    private int maxLength;

    public StringBinaryProperties() {
    }

    public StringBinaryProperties(int maxLength) {
      this.maxLength = maxLength;
    }

    public int getMaxLength() {
      return maxLength;
    }

    public void setMaxLength(int maxLength) {
      this.maxLength = maxLength;
    }
  }
}
