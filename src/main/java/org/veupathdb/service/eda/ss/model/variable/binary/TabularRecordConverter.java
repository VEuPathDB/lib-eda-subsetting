package org.veupathdb.service.eda.ss.model.variable.binary;

import java.nio.ByteBuffer;
import java.util.List;

public class TabularRecordConverter implements BinaryConverter<TabularRecord> {
  private final StringValueConverter entityIdStringConverter;
  private final List<StringValueConverter> ancestorIdStringConverters;
  private final List<BinaryConverter<Object>> variableValueConverters;
  private int size;

  public TabularRecordConverter(StringValueConverter entityIdStringConverter,
                                List<StringValueConverter> ancestorIdStringConverters,
                                List<BinaryConverter<Object>> variableValueConverters) {
    this.entityIdStringConverter = entityIdStringConverter;
    this.variableValueConverters = variableValueConverters;
    this.ancestorIdStringConverters = ancestorIdStringConverters;
    this.size = variableValueConverters.stream().mapToInt(BinarySerializer::numBytes).sum()
        + entityIdStringConverter.numBytes()
        + ancestorIdStringConverters.stream().mapToInt(BinarySerializer::numBytes).sum();
  }

  @Override
  public TabularRecord fromBytes(byte[] bytes) {
    return null;
  }

  @Override
  public TabularRecord fromBytes(byte[] bytes, int offset) {
    return null;
  }

  @Override
  public TabularRecord fromBytes(ByteBuffer buffer) {
    return null;
  }

  @Override
  public byte[] toBytes(TabularRecord varValue) {
    byte[] result = new byte[size];
    int index = 0;
    byte[] entityId = entityIdStringConverter.toBytes(varValue.getEntityId());
    System.arraycopy(entityId, 0, result, index, entityId.length);
    index += entityId.length;

    for (int i = 0; i < ancestorIdStringConverters.size(); i++) {
      byte[] ancestorId = ancestorIdStringConverters.get(i).toBytes(varValue.getAncestorIds().get(i));
      System.arraycopy(ancestorId, 0, result, index, entityId.length);
      index += ancestorId.length;
    }

    for (int i = 0; i < variableValueConverters.size(); i++) {
      byte[] record = variableValueConverters.get(i).toBytes(varValue.getVarValues().get(i));
      System.arraycopy(record, 0, result, index, entityId.length);
      index += record.length;
    }

    return result;
  }

  @Override
  public int numBytes() {
    return size;
  }
}
