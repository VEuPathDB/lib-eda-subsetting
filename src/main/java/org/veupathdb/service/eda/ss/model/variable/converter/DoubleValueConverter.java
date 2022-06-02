package org.veupathdb.service.eda.ss.model.variable.converter;

import java.nio.ByteBuffer;

public class DoubleValueConverter implements ValueConverter<Double> {

  @Override
  public Double fromString(String s) {
    return Double.parseDouble(s);
  }

  @Override
  public byte[] toBytes(Double varValue) {
    return ByteBuffer.allocate(Float.BYTES).putDouble(varValue).array();
  }

  @Override
  public Double fromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getDouble();
  }

  @Override
  public int numBytes() {
    return Double.BYTES;
  }

}
