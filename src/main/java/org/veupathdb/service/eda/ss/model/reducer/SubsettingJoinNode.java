package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SubsettingJoinNode {

  private final List<FilteredValueFile<?, Long>> filters;
  private Entity entity;

  public SubsettingJoinNode(List<FilteredValueFile<?, Long>> filters) {
    this.filters = filters;
  }

  public Iterator<Long> reduce() {
    // TODO include children.
    final List<Iterator<Long>> idStreams = new ArrayList<>();
    for (FilteredValueFile<?, Long> file: filters) {
      idStreams.add(file);
    }
    final StreamIntersectMerger intersectMerger = new StreamIntersectMerger(idStreams);
    return intersectMerger;
  }

  public Entity getEntity() {
    return entity;
  }
}
