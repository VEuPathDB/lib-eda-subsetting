package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SubsettingJoinNode {
  private List<SubsettingJoinNode> children; // Ignore for now
  private List<FilteredValueStream> filters;
  private Entity entity;


  public SubsettingJoinNode(List<FilteredValueStream> filters) {
    this.filters = filters;
  }

  public Iterator<Integer> reduce() {
    // TODO include children.
    final List<Iterator<Integer>> idStreams = new ArrayList<>();
    for (FilteredValueStream file: filters) {
      idStreams.add(file.iterator());
    }
    final Comparator<Integer> comparator = Comparator.naturalOrder();
    final StreamIntersectMerger intersectMerger = new StreamIntersectMerger(idStreams, comparator);
    return intersectMerger;
  }
}
