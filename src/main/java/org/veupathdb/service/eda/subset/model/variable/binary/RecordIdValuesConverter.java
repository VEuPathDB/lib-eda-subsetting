package org.veupathdb.service.eda.subset.model.variable.binary;

import org.veupathdb.service.eda.subset.model.Entity;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RecordIdValuesConverter implements BinarySerializer<RecordIdValues>, BinaryDeserializer<RecordIdValues> {
  private final LongValueConverter idIndexConverter;
  private final StringValueConverter idConverter;
  private final List<StringValueConverter> ancestorConverters;

  public RecordIdValuesConverter(Entity entity, Map<String, Integer> bytesReservedByEntityId) {
    this(
      entity.getAncestorEntities().stream().map(e -> bytesReservedByEntityId.get(e.getId())).toList(),
      bytesReservedByEntityId.get(entity.getId())
    );
  }

  public RecordIdValuesConverter(List<Integer> bytesReservedPerAncestor, int bytesReservedForId) {
    this.idIndexConverter = new LongValueConverter();
    this.idConverter = new StringValueConverter(bytesReservedForId);
    this.ancestorConverters = bytesReservedPerAncestor.stream()
      .map(StringValueConverter::new)
      .toList();
  }

  @Override
  public byte[] toBytes(RecordIdValues recordIdValues) {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(numBytes());
    byteBuffer.putLong(recordIdValues.getIdIndex());
    byteBuffer.put(idConverter.toBytes(recordIdValues.getEntityId()));
    for (int i = 0; i < ancestorConverters.size(); i++) {
      // Find string value converter that aligns with the ID that needs to be converted.
      // Each ancestor may be encoded with a different number of bytes.
      final String ancestorId = recordIdValues.getAncestorIds().get(i);
      final StringValueConverter converter = ancestorConverters.get(i);
      byteBuffer.put(converter.toBytes(ancestorId));
    }
    return byteBuffer.array();
  }

  @Override
  public int numBytes() {
    int numAncestorBytes = ancestorConverters.stream().mapToInt(StringValueConverter::numBytes).sum();
    int numEntityIdBytes = idConverter.numBytes();
    // idIndex + id_string + ancestor_id_strings
    return Long.BYTES + numAncestorBytes + numEntityIdBytes;
  }

  @Override
  public RecordIdValues fromBytes(byte[] bytes) {
    return fromBytes(bytes, 0);
  }

  @Override
  public RecordIdValues fromBytes(byte[] bytes, int offset) {
    Long idIndex = idIndexConverter.fromBytes(bytes, offset);
    offset += Long.BYTES;
    // Find the entityId converter corresponding with the primary entity.
    String entityId = idConverter.fromBytes(bytes, offset);
    offset += idConverter.numBytes();
    List<String> ancestors = new ArrayList<>(ancestorConverters.size());
    for (StringValueConverter ancestorConverter: ancestorConverters) {
      ancestors.add(ancestorConverter.fromBytes(bytes, offset));
      offset += ancestorConverter.numBytes();
    }
    return new RecordIdValues(idIndex, entityId, ancestors);
  }

  @Override
  public RecordIdValues fromBytes(ByteBuffer bytes) {
    long idIndex = bytes.getLong();
    String entityId = idConverter.fromBytes(bytes);
    List<String> ancestors = new ArrayList<>();
    for (StringValueConverter ancestorConverter: ancestorConverters) {
      ancestors.add(ancestorConverter.fromBytes(bytes));
    }
    return new RecordIdValues(idIndex, entityId, ancestors);
  }

}

