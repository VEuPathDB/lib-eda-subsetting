package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.reducer.ancestor.EntityIdIndexIteratorConverter;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.Iterator;
import java.util.List;

/**
 * Root of the reducer entity tree. The root of the tree represents the entity for which values are to be output.
 * When the root is reduced, it recursively delegates to its childrens' reduce methods, causing them to:
 * 1. Map streams of idIndexes output by their children to idIndexes of the ancestor entity corresponding to this node.
 * 2. Apply filters on variable values belonging to the node's entity, outputting idIndexes
 * 3. Merge idIndexes mapped from children with idIndexes output by variable filters on this node and output a single stream of ids.
 */
public class DataFlowMapReduceTree {
  private final List<Iterator<VariableValueIdPair<?>>> valueStreams;
  private final SubsettingJoinNode rootNode;
  private final Iterator<VariableValueIdPair<List<Long>>> ancestorStream;

  public DataFlowMapReduceTree(List<Iterator<Long>> filteredStreams,
                               List<Iterator<VariableValueIdPair<?>>> valueStreams,
                               List<SubsettingJoinNode> children,
                               Entity entity,
                               Study study,
                               EntityIdIndexIteratorConverter entityIdIndexIteratorConverter,
                               Iterator<VariableValueIdPair<List<Long>>> ancestorStream) {
    this.valueStreams = valueStreams;
    this.rootNode = new SubsettingJoinNode(filteredStreams, children, entity, study, entityIdIndexIteratorConverter);
    this.ancestorStream = ancestorStream;
  }

  /**
   *
   * @param valueStreams
   * @param rootNode
   * @param ancestorStream Stream of ancestors to include in tabular output. Can be null if entity has no ancestors.
   */
  public DataFlowMapReduceTree(List<Iterator<VariableValueIdPair<?>>> valueStreams,
                               SubsettingJoinNode rootNode,
                               Iterator<VariableValueIdPair<List<Long>>> ancestorStream) {
    this.valueStreams = valueStreams;
    this.rootNode = rootNode;
    this.ancestorStream = ancestorStream;
  }

  /**
   * Merge the filtered idIndex streams and map them to the values provided in the {@link DataFlowMapReduceTree#valueStreams}.
   * @return
   */
  public Iterator<List<String>> reduce() {
    return new FormattedTabularRecordStreamer(valueStreams, rootNode.reduce(), ancestorStream, rootNode.getEntity());
  }
}
