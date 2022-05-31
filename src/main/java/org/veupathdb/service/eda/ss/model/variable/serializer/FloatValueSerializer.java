package org.veupathdb.service.eda.ss.model.variable.serializer;

import java.nio.ByteBuffer;

public class FloatValueSerializer implements ValueSerializer<Float> {

  @Override
  public byte[] toBytes(Float varValue) {
    return ByteBuffer.allocate(Float.BYTES).putFloat(varValue).array();
  }

  @Override
  public Float fromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getFloat();
  }

  @Override
  public int numBytes() {
    return Float.BYTES;
  }

}
