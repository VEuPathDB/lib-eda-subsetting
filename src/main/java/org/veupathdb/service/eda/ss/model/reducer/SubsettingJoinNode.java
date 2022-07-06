package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.reducer.ancestor.AncestorMapperFactory;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SubsettingJoinNode {
  private final List<Iterator<Long>> filters;
  private final List<SubsettingJoinNode> children;
  private final Entity entity;
  private final Study study;
  private final AncestorMapperFactory ancestorMapperFactory;

  public SubsettingJoinNode(List<Iterator<Long>> filteredStreams,
                            List<SubsettingJoinNode> children,
                            Entity entity,
                            Study study,
                            AncestorMapperFactory ancestorMapperFactory) {
    this.filters = filteredStreams;
    this.children = children;
    this.entity = entity;
    this.study = study;
    this.ancestorMapperFactory = ancestorMapperFactory;
  }

  public Iterator<Long> reduce() {
    // TODO include children.
    List<Iterator<Long>> childStreams = children.stream()
        .map(child -> ancestorMapperFactory.fromEntity(child.reduce(), study, child.getEntity(), entity))
        .collect(Collectors.toList());
    final Iterator<Long> joinedFilterStream = filters.size() == 1
        ? filters.get(0)
        : new StreamIntersectMerger(filters);
    final Iterator<Long> joinedChildStream = childStreams.size() == 1
        ? childStreams.get(0)
        : new StreamIntersectMerger(childStreams);
    if (childStreams.isEmpty()) {
      return joinedFilterStream;
    } else if (filters.isEmpty()) {
      return joinedChildStream;
    } else {
      return new StreamIntersectMerger(List.of(joinedFilterStream, joinedChildStream));
    }
  }

  public Entity getEntity() {
    return entity;
  }
}
