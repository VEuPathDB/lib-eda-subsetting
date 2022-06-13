package org.veupathdb.service.eda.ss.model.variable.converter;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.List;

public class AncestorFileDeserializer implements BinaryDeserializer<VariableValueIdPair<Long>> {
  private TupleSerializer<Long> tupleSerializer;
  private int ancestorIndex;

  public AncestorFileDeserializer(TupleSerializer<Long> tupleSerializer, int ancestorIndex) {
    this.tupleSerializer = tupleSerializer;
    this.ancestorIndex = ancestorIndex;
  }

  @Override
  public VariableValueIdPair<Long> fromBytes(byte[] bytes) {
    List<Long> ancestors = tupleSerializer.fromBytes(bytes);
    return new VariableValueIdPair<>(ancestors.get(0), ancestors.get(ancestorIndex));
  }

  @Override
  public int numBytes() {
    return tupleSerializer.numBytes() * tupleSerializer.getLength();
  }
}
