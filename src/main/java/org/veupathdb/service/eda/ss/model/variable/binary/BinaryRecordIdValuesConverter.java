package org.veupathdb.service.eda.ss.model.variable.binary;

import org.veupathdb.service.eda.ss.model.Entity;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BinaryRecordIdValuesConverter implements BinarySerializer<BinaryRecordIdValues>, BinaryDeserializer<BinaryRecordIdValues> {
  private LongValueConverter idIndexConverter;
  private ByteArrayConverter idConverter;
  private List<ByteArrayConverter> ancestorConverters;

  public BinaryRecordIdValuesConverter(Entity entity, Map<String, Integer> bytesReservedByEntityId) {
    this(
        entity.getAncestorEntities().stream().map(e -> bytesReservedByEntityId.get(e.getId())).collect(Collectors.toList()),
        bytesReservedByEntityId.get(entity.getId())
    );
  }

  public BinaryRecordIdValuesConverter(List<Integer> bytesReservedPerAncestor, int bytesReservedForId) {
    this.idIndexConverter = new LongValueConverter();
    this.idConverter = new ByteArrayConverter(bytesReservedForId);
    this.ancestorConverters = bytesReservedPerAncestor.stream()
        .map(ancestorNumBytes -> new ByteArrayConverter(ancestorNumBytes))
        .collect(Collectors.toList());
  }

  @Override
  public byte[] toBytes(BinaryRecordIdValues recordIdValues) {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(numBytes());
    byteBuffer.putLong(recordIdValues.getIdIndex());
    byteBuffer.put(idConverter.toBytes(recordIdValues.getEntityId()));
    for (int i = 0; i < ancestorConverters.size(); i++) {
      // Find string value converter that aligns with the ID that needs to be converted.
      // Each ancestor may be encoded with a different number of bytes.
      final byte[] ancestorId = recordIdValues.getAncestorIds().get(i);
      final ByteArrayConverter converter = ancestorConverters.get(i);
      byteBuffer.put(converter.toBytes(ancestorId));
    }
    return byteBuffer.array();
  }

  @Override
  public int numBytes() {
    int numAncestorBytes = ancestorConverters.stream().collect(Collectors.summingInt(ByteArrayConverter::numBytes));
    int numEntityIdBytes = idConverter.numBytes();
    // idIndex + id_string + ancestor_id_strings
    return Long.BYTES + numAncestorBytes + numEntityIdBytes;
  }

  @Override
  public BinaryRecordIdValues fromBytes(byte[] bytes) {
    return fromBytes(bytes, 0);
  }

  @Override
  public BinaryRecordIdValues fromBytes(byte[] bytes, int offset) {
    Long idIndex = idIndexConverter.fromBytes(bytes, offset);
    offset += Long.BYTES;
    // Find the entityId converter corresponding with the primary entity.
    byte[] entityId = idConverter.fromBytes(bytes, offset);
    offset += idConverter.numBytes();
    List<byte[]> ancestors = new ArrayList<>();
    for (ByteArrayConverter ancestorConverter: ancestorConverters) {
      ancestors.add(ancestorConverter.fromBytes(bytes, offset));
      offset += ancestorConverter.numBytes();
    }
    return new BinaryRecordIdValues(idIndex, entityId, ancestors);
  }

  @Override
  public BinaryRecordIdValues fromBytes(ByteBuffer bytes) {
    Long idIndex = bytes.getLong();
    byte[] entityId = idConverter.fromBytes(bytes);
    List<byte[]> ancestors = new ArrayList<>();
    for (ByteArrayConverter ancestorConverter: ancestorConverters) {
      ancestors.add(ancestorConverter.fromBytes(bytes));
    }
    return new BinaryRecordIdValues(idIndex, entityId, ancestors);
  }

}

