package org.veupathdb.service.eda.subset.testutil;

import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.subset.model.variable.binary.ValueWithIdSerializer;

import java.io.IOException;
import java.io.OutputStream;

public class VariableWriter<T> implements AutoCloseable {

  private final OutputStream outputStream;
  private final ValueWithIdSerializer<T> variableSerializer;

  public VariableWriter(final OutputStream outputStream, final ValueWithIdSerializer<T> byteConverter) {
    this.outputStream = outputStream;
    this.variableSerializer = byteConverter;
  }

  public void writeVar(VariableValueIdPair<T> variable) {
    try {
      outputStream.write(variableSerializer.toBytes(variable));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    this.outputStream.close();
  }
}
