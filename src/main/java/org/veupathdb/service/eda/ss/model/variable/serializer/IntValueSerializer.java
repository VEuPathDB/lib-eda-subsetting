package org.veupathdb.service.eda.ss.model.variable.serializer;

import java.nio.ByteBuffer;

public class IntValueSerializer implements ValueSerializer<Integer> {

  @Override
  public byte[] toBytes(Integer varValue) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(varValue).array();
  }

  @Override
  public Integer fromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getInt();
  }

  @Override
  public int numBytes() {
    return Integer.BYTES;
  }

}
