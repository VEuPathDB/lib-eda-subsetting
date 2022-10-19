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
public class BinaryRecordIdValues extends VariableValueIdPair<List<byte[]>> {
  private long idIndex;
  private byte[] entityId;
  private List<byte[]> ancestorIds;

  public BinaryRecordIdValues(long idIndex, byte[] entityId, List<byte[]> ancestorIds) {
    super(idIndex, new ListBuilder<>(entityId).addAll(ancestorIds).toList());
    this.idIndex = idIndex;
    this.entityId = entityId;
    this.ancestorIds = ancestorIds;
  }

  public long getIdIndex() {
    return idIndex;
  }

  public byte[] getEntityId() {
    return entityId;
  }

  public List<byte[]> getAncestorIds() {
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
