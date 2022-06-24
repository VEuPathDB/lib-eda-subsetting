package org.veupathdb.service.eda.ss.model.variable.binary;

import java.nio.ByteBuffer;

public class DoubleValueConverter implements BinaryConverter<Double> {

  @Override
  public byte[] toBytes(Double varValue) {
    return ByteBuffer.allocate(Float.BYTES).putDouble(varValue).array();
  }

  @Override
  public Double fromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getDouble();
  }

  @Override
  public Double fromBytes(byte[] bytes, int offset) {
    return ByteBuffer.wrap(bytes)
        .position(offset)
        .getDouble();
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
