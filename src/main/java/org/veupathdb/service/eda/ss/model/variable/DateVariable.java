package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.gusdb.fgputil.FormatUtil;
import org.veupathdb.service.eda.ss.Utils;
import org.veupathdb.service.eda.ss.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.function.Function;

public class DateVariable extends VariableWithValues<Long> {
  private final DateDistributionConfig _distributionConfig;

  public DateVariable(Variable.Properties varProperties, VariableWithValues.Properties valueProperties, DateDistributionConfig distributionConfig) {
    super(varProperties, valueProperties);
    _distributionConfig = distributionConfig;
    validateType(VariableType.DATE);
  }

  public static BinaryConverter<Long> getGenericBinaryConverter(BinaryProperties empty) {
    return new LongValueConverter();
  }

  @Override
  public BinaryProperties getBinaryProperties() {
    return new EmptyBinaryProperties();
  }

  @Override
  public BinaryConverter<Long> getBinaryConverter() {
    return getGenericBinaryConverter(null);
  }

  @Override
  public BinaryConverter<String> getStringConverter() {
    return new StringValueConverter(24);
  }

  @Override
  public Function<byte[], byte[]> getRawUtf8BinaryFormatter(TabularReportConfig tabularReportConfig) {
    // If we need to trip time from date vars, return everything between padding in the beginning and the first
    // occurrence of the byte 'T'.
    if (tabularReportConfig.getTrimTimeFromDateVars()) {
      return utf8Bytes -> {
        int stringLength = Utils.getPaddedUtf8StringLength(utf8Bytes);
        int endIndex = utf8Bytes.length + Integer.BYTES;
        for (int i = Integer.BYTES; i < stringLength + Integer.BYTES; i++) {
          if (utf8Bytes[i] == 'T') {
            endIndex = i;
            break;
          }
        }
        // If our variable is multi-valued, quote the resulting utf-8 bytes.
        if (getIsMultiValued()) {
          int actualStringLength = endIndex - Integer.BYTES;
          int quotedStringLength = endIndex - Integer.BYTES + 2;
          byte[] quoted = new byte[quotedStringLength];
          quoted[0] = '"';
          quoted[quoted.length - 1] = '"';
          System.arraycopy(utf8Bytes, Integer.BYTES, quoted, 1, actualStringLength);
          return quoted;
        }
        // Our variable is single-valued. Extract the bytes between the padding at the beginning for the size and the
        // end index.
        return Arrays.copyOfRange(utf8Bytes, Integer.BYTES, endIndex);
      };
    } else {
      return super.getRawUtf8BinaryFormatter(tabularReportConfig);
    }
  }

  @Override
  public Long fromString(String s) {
    return FormatUtil.parseDateTime(s).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  @Override
  public String valueToString(Long val, TabularReportConfig reportConfig) {
    if (reportConfig.getTrimTimeFromDateVars()) {
      return DateTimeFormatter.ISO_LOCAL_DATE.format(Instant.ofEpochMilli(val).atOffset(ZoneOffset.UTC));
    }
    return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(Instant.ofEpochMilli(val).atOffset(ZoneOffset.UTC));
  }

  public DateDistributionConfig getDistributionConfig() {
    return _distributionConfig;
  }

  public static DateVariable assertType(Variable variable) {
    if (variable instanceof DateVariable) return (DateVariable)variable;
    throw new BadRequestException("Variable " + variable.getId() +
        " of entity " + variable.getEntityId() + " is not a date variable.");
  }
}
