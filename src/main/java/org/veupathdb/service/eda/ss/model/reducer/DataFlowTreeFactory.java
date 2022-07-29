package org.veupathdb.service.eda.ss.model.reducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.Filter;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Constructs data flow trees for tabular subsetting requests. These trees represent the flow of data for a particular
 * request. The tree will be rooted with the entity for which we are outputting one or more variables.
 */
public class DataFlowTreeFactory {
  private static final Logger LOG = LogManager.getLogger(DataFlowTreeFactory.class);

  public TreeNode<DataFlowNodeContents> create(TreeNode<Entity> prunedEntityTree,
                                               Entity outputEntity,
                                               List<Filter> filters,
                                               Study study) {
    final TreeNode<Entity> outputNode = prunedEntityTree.findFirst(entity -> entity.equals(outputEntity));
    // In lieu of a pointer up the tree, we use this function to traverse from the original root to a node's parent.
    final Function<TreeNode<Entity>, TreeNode<Entity>> parentMapper = child ->
        prunedEntityTree.findFirst(candidate -> candidate.getChildNodes().contains(child), null);
    final TreeNode<DataFlowNodeContents> newRoot = rerootTree(x -> true, study, outputNode, parentMapper, filters);
    return newRoot;
  }

  /**
   * Takes a study's entity tree and re-roots it starting at the output node. This is done by doing a graph traversal
   * from the output node going both up to the parent node and down to the children.
   *
   * @param shouldTraverseNode The previously traversed node, used to ensure we don't bounce back and forth between nodes.
   * @param study Study associated with entity diagram.
   * @param currentTraversalNode The current node in the traverasal.
   * @param parentRetriever Function mapping from a node to its parent in the original tree.
   * @param filters All filters in the original subsetting request.
   * @return
   */
  private TreeNode<DataFlowNodeContents> rerootTree(Predicate<TreeNode<Entity>> shouldTraverseNode,
                                                    Study study,
                                                    TreeNode<Entity> currentTraversalNode,
                                                    Function<TreeNode<Entity>, TreeNode<Entity>> parentRetriever,
                                                    List<Filter> filters) {
    // Collect filters applicable to the current entity.
    final List<Filter> applicableFilters = filters.stream()
        .filter(candidate -> candidate.getEntity().equals(currentTraversalNode.getContents()))
        .collect(Collectors.toList());
    // Convert the entity node to a data flow node.
    final DataFlowNodeContents contents = new DataFlowNodeContents(
        applicableFilters,
        currentTraversalNode.getContents(),
        study
    );
    TreeNode<DataFlowNodeContents> newRoot = new TreeNode<>(contents);
    TreeNode<Entity> parent = parentRetriever.apply(currentTraversalNode);
    if (parent != null && shouldTraverseNode.test(parent)) {
      newRoot.addChildNode(rerootTree(node -> node != currentTraversalNode, study, parent, parentRetriever, filters));
    }
    for (TreeNode<Entity> child : currentTraversalNode.getChildNodes()) {
      // Since we traverse the parent node in addition to children, ensure we don't traverse backwards.
      if (shouldTraverseNode.test(child)) {
        newRoot.addChildNode(rerootTree(node -> node != currentTraversalNode, study, child, parentRetriever, filters));
      }
    }
    return newRoot;
  }
}
