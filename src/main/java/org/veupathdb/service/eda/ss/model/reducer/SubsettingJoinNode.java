package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.reducer.ancestor.AncestorMapperFactory;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Node representing an entity in the map-reduce entity tree. The structure of the tree is a function of the study's
 * entity hierarchy and the output variable. Opens value streams for all variables on which we are filtering for this
 * entity and intersects all of the ID indexes that are not filtered. Recursively reduce each child node in this manner
 * and map the child ID Indexes to this element's ID indexes.
 */
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
