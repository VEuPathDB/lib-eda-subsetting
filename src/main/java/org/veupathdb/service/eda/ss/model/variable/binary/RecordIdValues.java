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
public class RecordIdValues extends VariableValueIdPair<List<String>> {
  private long idIndex;
  private String entityId;
  private List<String> ancestorIds;
  
  public RecordIdValues(long idIndex, String entityId, List<String> ancestorIds) {
    super(idIndex, new ListBuilder<>(entityId).addAll(ancestorIds).toList());
    this.idIndex = idIndex;
    this.entityId = entityId;
    this.ancestorIds = ancestorIds;
  }

  public long getIdIndex() {
    return idIndex;
  }

  public String getEntityId() {
    return entityId;
  }

  public List<String> getAncestorIds() {
    return ancestorIds;
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof RecordIdValues)) return false;
    RecordIdValues recordIdValues = (RecordIdValues)object;
    return (recordIdValues.idIndex == idIndex
        && recordIdValues.entityId.equals(entityId)
        && Objects.equals(recordIdValues.ancestorIds, ancestorIds));
  }
  
  @Override
  public String toString() {
    return "idIndex: " + idIndex + " entityId: " + entityId + " ancestorIds: " + String.join(", ", ancestorIds);
  }
  
}
