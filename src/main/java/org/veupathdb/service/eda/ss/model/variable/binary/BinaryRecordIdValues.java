package org.veupathdb.service.eda.ss.model.variable.binary;
import org.gusdb.fgputil.ListBuilder;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.List;
import java.util.Objects;

/**
 * Record containing an idIndex, its entity_id and its ancestor's entity_ids
 * @author sfischer
 *
 */
public class BinaryRecordIdValues extends VariableValueIdPair<byte[][]> {
  private long idIndex;
  private byte[] entityId;
  private byte[][] ancestorIds;

  public BinaryRecordIdValues(long idIndex, byte[] entityId, byte[][] ancestorIds) {
    super(idIndex, constructList(entityId, ancestorIds));
    this.idIndex = idIndex;
    this.entityId = entityId;
    this.ancestorIds = ancestorIds;
  }

  private static byte[][] constructList(byte[] entityId, byte[][] ancestorIds) {
    byte[][] outputList = new byte[ancestorIds.length + 1][];
    outputList[0] = new byte[entityId.length];
    for (int i = 0; i < entityId.length; i++) {
      outputList[0][i] = entityId[i];
    }
    for (int i = 0; i < ancestorIds.length; i++) {
      outputList[i + 1] = new byte[ancestorIds[i].length];
      for (int j = 0; j < ancestorIds[i].length; j++) {
        outputList[i + 1][j] = ancestorIds[i][j];
      }
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
    if (!(object instanceof BinaryRecordIdValues)) return false;
    BinaryRecordIdValues recordIdValues = (BinaryRecordIdValues)object;
    return (recordIdValues.idIndex == idIndex
        && recordIdValues.entityId.equals(entityId)
        && Objects.equals(recordIdValues.ancestorIds, ancestorIds));
  }
  
  @Override
  public String toString() {
    return "idIndex: " + idIndex + " entityId: " + entityId + " ancestorIds: ";
  }
  
}
