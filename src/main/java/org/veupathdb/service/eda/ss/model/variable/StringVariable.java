package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.gusdb.fgputil.FormatUtil;
import org.veupathdb.service.eda.ss.Utils;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.ByteArrayConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;

import java.util.Arrays;

public class StringVariable extends VariableWithValues<byte[]> {
  private StringBinaryProperties binaryProperties;

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

  @Override
  public String valueToJsonText(byte[] val, TabularReportConfig config) {
    return quote(valueToString(val, config));
  }

  @Override
  public byte[] valueToJsonTextBytes(byte[] val, TabularReportConfig config) {
    byte[] utf8Bytes = valueToUtf8Bytes(val, config);
    byte[] quoted = new byte[val.length + 2];
    quoted[0] = '"';
    quoted[quoted.length - 1] = '"';
    for (int i = 0; i < utf8Bytes.length; i++) {
      quoted[i + 1] = utf8Bytes[i];
    }
    return quoted;
  }

  @Override
  public byte[] valueToUtf8Bytes(byte[] val, TabularReportConfig config) {
    int length = Utils.getPaddedUtf8StringLength(val);
    return Arrays.copyOfRange(val, Integer.BYTES, length + Integer.BYTES);
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
