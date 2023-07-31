package org.veupathdb.service.eda.ss.model.reducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.functional.Functions;
import org.gusdb.fgputil.functional.TreeNode;
import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.reducer.ancestor.EntityIdIndexIteratorConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Traverses a data flow map-reduce tree to provide a stream of potentially filtered entity ID indexes for a subsetting
 * request.
 */
public class DataFlowTreeCountReducer extends DataFlowTreeReducer {
  private static final Logger LOG = LogManager.getLogger(DataFlowTreeCountReducer.class);

  private EntityIdIndexIteratorConverter entityIdIndexIteratorConverter;
  private BinaryValuesStreamer binaryValuesStreamer;
  private Map<String, CountingCloseableIterator<Long>> entityCounts;

  public DataFlowTreeCountReducer(EntityIdIndexIteratorConverter entityIdIndexIteratorConverter,
                                  BinaryValuesStreamer binaryValuesStreamer) {
    super(entityIdIndexIteratorConverter, binaryValuesStreamer);
    entityCounts = new HashMap<>();
//    this.entityIdIndexIteratorConverter = entityIdIndexIteratorConverter;
//    this.binaryValuesStreamer = binaryValuesStreamer;
  }

  public Map<String, CountingCloseableIterator<Long>> getEntityCounts() {
    return entityCounts;
  }

  @Override
  public CloseableIterator<Long> reduce(TreeNode<DataFlowNodeContents> root) {
    final CountingCloseableIterator<Long> outputStream = new CountingCloseableIterator<>(super.reduce(root));
    entityCounts.put(root.getContents().getEntity().getId(), outputStream);
    return outputStream;
  }

    public CloseableIterator<Long> reduce(TreeNode<DataFlowNodeContents> root, Map<String, CountingCloseableIterator<Long>> children) {
//    final List<CloseableIterator<Long>> allStreams = new ArrayList<>();
//    DataFlowNodeContents contents = root.getContents();
//
//    // Recursively call reduce() on children, converting the child's entity ID indexes to this entity's.
//    root.getChildNodes().stream()
//        .map(child -> entityIdIndexIteratorConverter.fromEntity(reduce(child, children), contents.getStudy(),
//            child.getContents().getEntity(),
//            contents.getEntity()))
//        .forEach(allStreams::add);
//
//    // Retrieve streams for this entity based on subsetting request spec.
//    root.getContents().getFilters().stream()
//        .map(Functions.fSwallow(filter -> filter.streamFilteredIds(binaryValuesStreamer, contents.getStudy())))
//        .forEach(allStreams::add);
//
//    if (root.getContents().shouldIncludeUnfilteredStream()) {
//      try {
//        allStreams.add(binaryValuesStreamer.streamUnfilteredEntityIdIndexes(contents.getStudy(), contents.getEntity()));
//      } catch (IOException e) {
//        throw new RuntimeException(e);
//      }
//    }
//
//    // Merge all entity ID index data streams if there are more than one.
//    if (allStreams.size() == 1) {
//      // If there is one stream, returned it but ensure that if an entity ID index appears multiple times, it is only
//      // returned once in the resulting stream to account for multi-value variables.
//      final CountingCloseableIterator<Long> outputStream = new CountingCloseableIterator<>(new StreamDeduper(allStreams.get(0)));
//      children.put(contents.getEntity().getId(), outputStream);
//      return outputStream;
//    }
    final CountingCloseableIterator<Long> outputStream = new CountingCloseableIterator<>(super.reduce(root));
    children.put(root.getContents().getEntity().getId(), outputStream);
    return outputStream;
  }
}
