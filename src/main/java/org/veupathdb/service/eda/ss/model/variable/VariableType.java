package org.veupathdb.service.eda.ss.model.variable;

import java.sql.Date;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;
import org.gusdb.fgputil.functional.Functions;
import org.json.JSONArray;
import org.veupathdb.service.eda.ss.model.varcollection.CollectionType;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;

public enum VariableType {
  STRING("string_value", "string", rs -> rs.getString("string_value"),
      VariableType::StringListToJsonStringArray, StringVariable::getGenericBinaryConverter),

  NUMBER("number_value", "number", rs -> doubleValueOrNull(rs, rs.getDouble("number_value")),
      VariableType::StringListToJsonFloatArray, FloatingPointVariable::getGenericBinaryConverter),
  
  INTEGER("number_value", "integer", rs -> integerValueOrNull(rs, rs.getLong("number_value")),
      VariableType::StringListToJsonIntegerArray, IntegerVariable::getGenericBinaryConverter),
  
  DATE("date_value", "date", rs -> dateValueOrNull(rs.getDate("date_value")),
      VariableType::StringListToJsonStringArray, DateVariable::getGenericBinaryConverter),
  
  LONGITUDE("number_value", "longitude", rs -> doubleValueOrNull(rs, rs.getDouble("number_value")),
      VariableType::StringListToJsonFloatArray, LongitudeVariable::getGenericBinaryConverter);

  private final String tallTableColumnName;
  private final String typeString;
  private final FunctionWithException<ResultSet, String> resultSetToStringValue;
  private final FunctionWithException<List<String>, JSONArray> multiValStringListToJsonArray;
  private final Supplier<BinaryConverter<?>> converterSupplier;

  VariableType(String tallTableColumnName,
               String typeString,
               FunctionWithException<ResultSet, String> resultSetToStringValue,
               FunctionWithException<List<String>, JSONArray> multiValStringListToJsonArray,
               Supplier<BinaryConverter<?>> converterSupplier) {
    this.tallTableColumnName = tallTableColumnName;
    this.resultSetToStringValue = resultSetToStringValue;
    this.multiValStringListToJsonArray = multiValStringListToJsonArray;
    this.typeString = typeString;
    this.converterSupplier = converterSupplier;
  }

  public String getTallTableColumnName() {
    return this.tallTableColumnName;
  }

  public static VariableType fromString(String str) {
    if (str.equals(STRING.typeString)) return STRING; 
    else if (str.equals(NUMBER.typeString)) return NUMBER;
    else if (str.equals(INTEGER.typeString)) return INTEGER;
    else if (str.equals(LONGITUDE.typeString)) return LONGITUDE;
    else if (str.equals(DATE.typeString)) return DATE;
    else throw new RuntimeException("Illegal variable type string: " + str);
  }

  public String convertRowValueToStringValue(ResultSet rs) {
    try {
      return resultSetToStringValue.apply(rs);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public JSONArray convertStringListToJsonArray(List<String> multipleValuesAsStrings) {
    try {
      return multiValStringListToJsonArray.apply(multipleValuesAsStrings);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // utility to convert null DB double values to real null
  private static String doubleValueOrNull(ResultSet rs, double value) {
    return Functions.swallowAndGet(() -> rs.wasNull() ? null : String.valueOf(value));
  }

  // utility to convert null DB integer values to real null
  private static String integerValueOrNull(ResultSet rs, long value) {
    return Functions.swallowAndGet(() -> rs.wasNull() ? null : String.valueOf(value));
  }

  // utility to convert null DB date values to real null
  private static String dateValueOrNull(Date value) {
    return value == null ? null : FormatUtil.formatDateTimeNoTimezone(value);
  }
  
  // convert a list of multiple values, in string form, to a parallel string JSON array
  private static JSONArray StringListToJsonStringArray(List<String> stringList) {
    return new JSONArray(stringList);
  }
  
  // convert a list of multiple floating-point values, in string form, to a parallel number JSON array
  private static JSONArray StringListToJsonFloatArray(List<String> stringList) {
    List<Double> numberList = new ArrayList<>(stringList.size());
    for (String valueAsString : stringList) {
      numberList.add(Double.valueOf(valueAsString));
    }
    return new JSONArray(numberList);
  }

  // convert a list of multiple integer values, in string form, to a parallel number JSON array
  private static JSONArray StringListToJsonIntegerArray(List<String> stringList) {
    List<Long> numberList = new ArrayList<>(stringList.size());
    for (String valueAsString : stringList) {
      numberList.add(Long.valueOf(valueAsString));
    }
    return new JSONArray(numberList);
  }

  public boolean isNumber() {
    return "number_value".equals(tallTableColumnName);
  }

  public boolean isSameTypeAs(CollectionType type) {
    return type.name().equals(name());
  }

  public String getTypeString() {
    return typeString;
  }
  
  public Supplier<BinaryConverter<?>> getConverterSupplier() {
    return converterSupplier;
  }
}
