package org.veupathdb.service.eda.ss.model.reducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.functional.Functions;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.model.filter.SingleValueFilter;
import org.veupathdb.service.eda.ss.model.reducer.ancestor.EntityIdIndexIteratorConverter;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Constructs data flow trees for tabular subsetting requests. These trees represent the flow of data for a particular
 * request. The tree will be rooted with the entity for which we are outputting one or more variables.
 */
public class DataFlowTreeFactory {
  private static final Logger LOG = LogManager.getLogger(DataFlowTreeFactory.class);

  private final BinaryValuesStreamer binaryValuesStreamer;
  private final EntityIdIndexIteratorConverter idIndexIteratorConverter;

  public DataFlowTreeFactory(BinaryValuesStreamer binaryValuesStreamer,
                             EntityIdIndexIteratorConverter idIndexIteratorConverter) {
    this.binaryValuesStreamer = binaryValuesStreamer;
    this.idIndexIteratorConverter = idIndexIteratorConverter;
  }

  /**
   * Creates a data-flow tree from the pruned entity tree. The constructed tree will be rooted with the outputEntity
   * passed in.
   * @param prunedEntityTree Entity tree pruned to only active nodes based on output and filters.
   * @param outputEntity Entity for which variables are output.
   * @param filters Filters to apply to entity stream.
   * @param outputVariables Variables to return for each output entity.
   * @param study Study for which
   * @return
   */
  public DataFlowMapReduceTree create(TreeNode<Entity> prunedEntityTree,
                                      Entity outputEntity,
                                      List<Filter> filters,
                                      List<Variable> outputVariables,
                                      Study study) {
    if (!outputVariables.stream().allMatch(var -> var.getEntity().equals(outputEntity))) {
      throw new IllegalArgumentException("All output variables must be associated with output entity.");
    }
    try {
      LOG.info("Creating map-reduce data-flow tree rooted with output entity: " + outputEntity.getId());
      final TreeNode<Entity> parent = prunedEntityTree.findFirst(entity -> entity.equals(outputEntity));
      final SubsettingJoinNode root = rerootTree(prunedEntityTree, parent, null, filters, study);
      final List<Iterator<VariableValueIdPair<?>>> outputStreams = outputVariables.stream()
          .map(Functions.fSwallow(varWithValues ->
              // Any variables without values (i.e. categories) should fail validation upstream.
              binaryValuesStreamer.streamIdValuePairs(study, (VariableWithValues<?>) varWithValues)))
          .collect(Collectors.toList());
      final Iterator<VariableValueIdPair<List<Long>>> ancestorStreams = outputEntity.getAncestorEntities().isEmpty()
          ? null
          : binaryValuesStreamer.streamAncestorIds(outputEntity, study);
      return new DataFlowMapReduceTree(outputStreams, root, ancestorStreams);
    } catch (IOException e) {
      throw new RuntimeException("Failed to open files to stream binary data", e);
    }
  }

  /**
   * Recursively depth-first traverse each child and then the parent.
   *
   * @param originalRoot The original entity tree's root. This is needed to find the parent of our current node.
   * @param currentTraversalNode Node that is currently being traversed.
   * @param previousNode Node that was previously traversed. We keep track of this to avoid traversing from whence we came.
   * @param filters List of all filters in request, used in construction of the {@link SubsettingJoinNode} corresponding to this node.
   * @param study Study that subsetting data is requested for.
   * @return
   */
  private SubsettingJoinNode rerootTree(TreeNode<Entity> originalRoot,
                                        TreeNode<Entity> currentTraversalNode,
                                        TreeNode<Entity> previousNode,
                                        List<Filter> filters,
                                        Study study) {
    List<SubsettingJoinNode> children = new ArrayList<>();
    if (!currentTraversalNode.isLeaf()) {
      for (TreeNode<Entity> child : currentTraversalNode.getChildNodes()) {
        // Since we traverse the parent node, ensure we don't traverse backwards.
        if (previousNode != child) {
          children.add(rerootTree(originalRoot, child, currentTraversalNode, filters, study));
        }
      }
    }
    // After traversing children, traverse the parent, adding the output of the traversal as one of our children.
    // Note that we don't capture the "orientation" of this edge at this point, but these edges are special in that
    // traversal requires expansion of the entity IDs to convert from the descendant to the ascendant.
    TreeNode<Entity> parent = originalRoot.findFirst(candidate -> candidate.getChildNodes().contains(currentTraversalNode), null);
    if (parent != null && previousNode != parent) {
      children.add(rerootTree(originalRoot, parent, currentTraversalNode, filters, study));
    }

    final List<Iterator<Long>> filteredValueStreams = filters.stream()
        .filter(filter -> filter instanceof SingleValueFilter) // TODO Handle MultiValueFilters, not sure how to do that yet.
        .filter(filter -> filter.getEntity() == currentTraversalNode.getContents()) // Filter to filters relevant to this node's entity
        .map(Functions.fSwallow(
            singleValueFilter -> binaryValuesStreamer.streamFilteredValues((SingleValueFilter<?, ?>) singleValueFilter, study)))
        .collect(Collectors.toList());
    return new SubsettingJoinNode(filteredValueStreams, children, currentTraversalNode.getContents(), study, idIndexIteratorConverter);
  }
}
