package org.veupathdb.service.eda.ss.model.reducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.reducer.ancestor.EntityIdIndexIteratorConverter;

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
  private static final Logger LOG = LogManager.getLogger(SubsettingJoinNode.class);

  private final List<Iterator<Long>> filters; // ID indexes of this Entity's "type" for filtered data streams.
  private final List<SubsettingJoinNode> children;
  private final Entity entity;
  private final Study study;
  private final EntityIdIndexIteratorConverter entityIdIndexIteratorConverter;

  public SubsettingJoinNode(List<Iterator<Long>> filteredStreams,
                            List<SubsettingJoinNode> children,
                            Entity entity,
                            Study study,
                            EntityIdIndexIteratorConverter entityIdIndexIteratorConverter) {
    this.filters = filteredStreams;
    this.children = children;
    this.entity = entity;
    this.study = study;
    this.entityIdIndexIteratorConverter = entityIdIndexIteratorConverter;
  }

  public Iterator<Long> reduce() {
    // Recursively call reduce() on children, converting the child's entity ID indexes to this entity's.
    List<Iterator<Long>> childStreams = children.stream()
        .map(child -> entityIdIndexIteratorConverter.fromEntity(child.reduce(), study, child.getEntity(), entity))
        .collect(Collectors.toList());
    // Merge the filtered data streams if there are more than one.
    final Iterator<Long> joinedFilterStream = filters.size() == 1
        ? filters.get(0)
        : new StreamIntersectMerger(filters);
    // Merge the child data streams if there are more than one.
    final Iterator<Long> joinedChildStream = childStreams.size() == 1
        ? childStreams.get(0)
        : new StreamIntersectMerger(childStreams);
    if (childStreams.isEmpty()) {
      LOG.debug("No child data streams found for {}, returning the output of the joined filter streams.", entity.getId());
      return joinedFilterStream;
    } else if (filters.isEmpty()) {
      LOG.debug("No filter data streams found for {}, returning the output of the child data streams.", entity.getId());
      return joinedChildStream;
    } else {
      LOG.debug("This node has data streams from filter requests and children, joining these streams to intersect them.");
      return new StreamIntersectMerger(List.of(joinedFilterStream, joinedChildStream));
    }
  }

  public Entity getEntity() {
    return entity;
  }
}
