package org.veupathdb.service.eda.ss.testutil;

import org.veupathdb.service.eda.ss.model.variable.VariableValue;
import org.veupathdb.service.eda.ss.model.variable.serializer.ValueWithIdSerializer;

import java.io.IOException;
import java.io.OutputStream;

public class VariableWriter<S,T> implements AutoCloseable {

  private OutputStream outputStream;
  private ValueWithIdSerializer<T> variableSerializer;

  public VariableWriter(final OutputStream outputStream, final ValueWithIdSerializer<T> byteConverter) {
    this.outputStream = outputStream;
    this.variableSerializer = byteConverter;
  }

  public void writeVar(VariableValue<T> variable) {
    try {
      outputStream.write(variableSerializer.convertToBytes(variable));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    this.outputStream.close();
  }
}
