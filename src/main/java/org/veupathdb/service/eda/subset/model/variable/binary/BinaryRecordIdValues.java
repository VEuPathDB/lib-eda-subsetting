package org.veupathdb.service.eda.subset.model.variable.binary;
import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;

import java.util.Arrays;

/**
 * Record containing an idIndex, its entity_id and its ancestor's entity_ids
 * @author sfischer
 */
public class BinaryRecordIdValues extends VariableValueIdPair<byte[][]> {
  private final long idIndex;
  private final byte[] entityId;
  private final byte[][] ancestorIds;

  public BinaryRecordIdValues(long idIndex, byte[] entityId, byte[][] ancestorIds) {
    super(idIndex, constructList(entityId, ancestorIds));
    this.idIndex = idIndex;
    this.entityId = entityId;
    this.ancestorIds = ancestorIds;
  }

  private static byte[][] constructList(byte[] entityId, byte[][] ancestorIds) {
    byte[][] outputList = new byte[ancestorIds.length + 1][];
    outputList[0] = Arrays.copyOf(entityId, entityId.length);
    for (int i = 0; i < ancestorIds.length; i++) {
      outputList[i + 1] = Arrays.copyOf(ancestorIds[i], ancestorIds[i].length);
    }
    return outputList;
  }

  public long getIdIndex() {
    return idIndex;
  }

  public byte[] getEntityId() {
    return entityId;
  }

  public byte[][] getAncestorIds() {
    return ancestorIds;
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof BinaryRecordIdValues recordIdValues)) return false;
    return (recordIdValues.idIndex == idIndex
      && Arrays.equals(recordIdValues.entityId, entityId)
      && Arrays.deepEquals(recordIdValues.ancestorIds, ancestorIds));
  }

  @Override
  public String toString() {
    return "idIndex: " + idIndex + " entityId: " + entityId + " ancestorIds: ";
  }

}
