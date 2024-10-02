package org.veupathdb.service.eda.subset.model.variable.binary;

import java.nio.ByteBuffer;

public class DoubleValueConverter implements BinaryConverter<Double> {
  private final LongValueConverter longValueConverter;

  public DoubleValueConverter() {
    this.longValueConverter = new LongValueConverter();
  }

  @Override
  public byte[] toBytes(Double varValue) {
    long longValue = Double.doubleToLongBits(varValue);
    return longValueConverter.toBytes(longValue);
  }

  @Override
  public Double fromBytes(byte[] bytes) {
    long longValue = longValueConverter.fromBytes(bytes);
    return Double.longBitsToDouble(longValue);
  }

  @Override
  public Double fromBytes(byte[] bytes, int offset) {
    long longValue = longValueConverter.fromBytes(bytes, offset);
    return Double.longBitsToDouble(longValue);
  }

  @Override
  public Double fromBytes(ByteBuffer buffer) {
    return buffer.getDouble();
  }

  @Override
  public int numBytes() {
    return Double.BYTES;
  }

}
