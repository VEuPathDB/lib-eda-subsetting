package org.veupathdb.service.eda.ss.model.variable.binary;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Binary deserializer that knows how to deserialize records with an entity identifier and all of that entity's
 * ancestors. Rather than returning all ancestors, it only returns a single ancestor indicated by {@link AncestorDeserializer#ancestorColumn}
 */
public class AncestorDeserializer implements BinaryDeserializer<VariableValueIdPair<Long>> {
  private ArrayConverter<Long> listConverter;
  private int ancestorColumn;

  /**
   * Creates a new instance to deserialize ancestor files, extracting only the entity ID and the ID of the ancestor
   * of interest specified by ancestorColumn.
   *
   * @param listConverter Tuple serializer used to deserialize all ancestors.
   * @param ancestorColumn Column number (0-indexed) of ancestor to be returned.
   */
  public AncestorDeserializer(ArrayConverter<Long> listConverter, int ancestorColumn) {
    if (ancestorColumn < 0 || ancestorColumn > listConverter.getSize()) {
      throw new IndexOutOfBoundsException("Ancestor column number " + ancestorColumn + " is out of bounds");
    }
    this.listConverter = listConverter;
    this.ancestorColumn = ancestorColumn;
  }

  @Override
  public VariableValueIdPair<Long> fromBytes(byte[] bytes) {
    Long[] ancestors = listConverter.fromBytes(bytes);
    return new VariableValueIdPair<>(ancestors[0], ancestors[ancestorColumn]);
  }

  @Override
  public VariableValueIdPair<Long> fromBytes(byte[] bytes, int offset) {
    ByteBuffer buf = ByteBuffer.wrap(bytes).position(offset);
    Long[] ancestors = listConverter.fromBytes(buf);
    return new VariableValueIdPair<>(ancestors[0], ancestors[ancestorColumn]);
  }

  @Override
  public VariableValueIdPair<Long> fromBytes(ByteBuffer buffer) {
    Long[] ancestors = listConverter.fromBytes(buffer);
    return new VariableValueIdPair<>(ancestors[0], ancestors[ancestorColumn]);
  }

  @Override
  public int numBytes() {
    return listConverter.numBytes();
  }
}
