package org.veupathdb.service.eda.ss.model.variable.serializer;

import org.veupathdb.service.eda.ss.model.variable.VariableValue;

import java.nio.ByteBuffer;

public class ValueWithIdSerializer<T> {

  private final ValueSerializer<T> _valueSerializer;

  public ValueWithIdSerializer(ValueSerializer<T> valueSerializer) {
    _valueSerializer = valueSerializer;
  }

  /**
   * Converts a variable to an array of bytes. The first 4 bytes are written as an integer variable identifier and the
   * next N are read as the variable value.
   * @param variable to convert to bytes
   * @return Deserialized variable object
   */
  public byte[] convertToBytes(VariableValue<T> variable) {
    final int bufferSize = totalBytesNeeded();
    final ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
    byteBuffer.putInt(variable.getEntityId());
    byteBuffer.put(_valueSerializer.toBytes(variable.getValue()));
    return byteBuffer.array();
  }

  /**
   * Converts an array of bytes to a variable. The first 4 bytes are read as an integer and the next N are read as
   * the variable value.
   * @param bytes to convert to variable
   * @return Deserialized variable object
   */
  public VariableValue<T> convertFromBytes(byte[] bytes) {
//    if (bytes.length != totalBytesNeeded()) {
//      throw new IllegalArgumentException("Expected byte array of size, " + totalBytesNeeded() + " but found: "
//          + bytes.length);
//    }
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    final int varId = byteBuffer.getInt();
    final byte[] varValueBytes = new byte[_valueSerializer.numBytes()];
    byteBuffer.get(varValueBytes);
    final T varValue = _valueSerializer.fromBytes(varValueBytes);
    return new VariableValue<>(varId, varValue);
  }

  public int totalBytesNeeded() {
    // Keep 4 for the variable identifier and the rest for the value.
    return Integer.BYTES + _valueSerializer.numBytes();
  }

}

