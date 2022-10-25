package org.veupathdb.service.eda.ss.model.reducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.functional.Functions;
import org.gusdb.fgputil.functional.TreeNode;
import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.reducer.ancestor.EntityIdIndexIteratorConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Traverses a data flow map-reduce tree to provide a stream of potentially filtered entity ID indexes for a subsetting
 * request.
 */
public class DataFlowTreeReducer {
  private static final Logger LOG = LogManager.getLogger(DataFlowTreeReducer.class);

  private EntityIdIndexIteratorConverter entityIdIndexIteratorConverter;
  private BinaryValuesStreamer binaryValuesStreamer;

  public DataFlowTreeReducer(EntityIdIndexIteratorConverter entityIdIndexIteratorConverter,
                             BinaryValuesStreamer binaryValuesStreamer) {
    this.entityIdIndexIteratorConverter = entityIdIndexIteratorConverter;
    this.binaryValuesStreamer = binaryValuesStreamer;
  }

  public CloseableIterator<Long> reduce(TreeNode<DataFlowNodeContents> root) {
    final List<CloseableIterator<Long>> allStreams = new ArrayList<>();
    DataFlowNodeContents contents = root.getContents();

    // Recursively call reduce() on children, converting the child's entity ID indexes to this entity's.
    root.getChildNodes().stream()
        .map(child -> entityIdIndexIteratorConverter.fromEntity(reduce(child), contents.getStudy(),
            child.getContents().getEntity(),
            contents.getEntity()))
        .forEach(allStreams::add);

    // Retrieve streams for this entity based on subsetting request spec.
    root.getContents().getFilters().stream()
        .map(Functions.fSwallow(filter -> filter.streamFilteredIds(binaryValuesStreamer, contents.getStudy())))
        .forEach(allStreams::add);

    if (!root.getContents().getUnfilteredOutputVars().isEmpty()) {
      try {
        allStreams.add(binaryValuesStreamer.streamUnfilteredEntityIdIndexes(contents.getStudy(), contents.getEntity()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    // Merge all entity ID index data streams if there are more than one.
    if (allStreams.size() == 1) {
      // If there is one stream, returned it but ensure that if an entity ID index appears multiple times, it is only
      // returned once in the resulting stream to account for multi-value variables.
      return new StreamDeduper(allStreams.get(0));
    }
    return new StreamIntersectMerger(allStreams);
  }
}
