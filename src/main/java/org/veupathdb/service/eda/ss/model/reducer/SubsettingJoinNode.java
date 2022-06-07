package org.veupathdb.service.eda.ss.model.reducer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SubsettingJoinNode {

  private final List<FilteredValueStream<?>> filters;

  public SubsettingJoinNode(List<FilteredValueStream<?>> filters) {
    this.filters = filters;
  }

  public Iterator<Long> reduce() {
    // TODO include children.
    final List<Iterator<Long>> idStreams = new ArrayList<>();
    for (FilteredValueStream<?> file: filters) {
      idStreams.add(file);
    }
    final Comparator<Long> comparator = Comparator.naturalOrder();
    final StreamIntersectMerger<Long> intersectMerger = new StreamIntersectMerger<>(idStreams, comparator);
    return intersectMerger;
  }
}
