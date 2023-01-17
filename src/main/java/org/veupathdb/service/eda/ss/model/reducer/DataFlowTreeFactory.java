package org.veupathdb.service.eda.ss.model.reducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
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
                                               List<VariableWithValues> outputVariables,
                                               Study study) {
    final TreeNode<Entity> outputNode = prunedEntityTree.findFirst(entity -> entity.equals(outputEntity));
    // In lieu of a pointer up the tree, we use this function to traverse from the original root to a node's parent.
    final Function<TreeNode<Entity>, Optional<TreeNode<Entity>>> parentMapper = child ->
        Optional.ofNullable(prunedEntityTree.findFirst(candidate -> candidate.getChildNodes().contains(child), null));
    return rerootTree(parentMapper, outputNode, null, filters, outputVariables, study);
  }

  /**
   * Takes a study's entity tree and re-roots it starting at the output node. This is done by doing a graph traversal
   * from the output node going both up to the parent node and down to the children.
   *
   * @param parentRetriever Function mapping from a node to its parent in the original tree. Returns an empty optional
   *                        if the node is the root of the tree and does not have a parent.
   * @param currentTraversalNode The current node in the traverasal.
   * @param previousNode The previously traversed node, used to ensure we don't bounce back and forth between nodes.
   * @param filters All filters in the original subsetting request.
   * @param study Study associated with entity diagram.
   * @return
   */
  private TreeNode<DataFlowNodeContents> rerootTree(Function<TreeNode<Entity>, Optional<TreeNode<Entity>>> parentRetriever,
                                                    TreeNode<Entity> currentTraversalNode,
                                                    TreeNode<Entity> previousNode,
                                                    List<Filter> filters,
                                                    List<VariableWithValues> outputVariables,
                                                    Study study) {
    // Collect filters applicable to the current entity.
    final List<Filter> applicableFilters = filters.stream()
        .filter(candidate -> candidate.getEntity().equals(currentTraversalNode.getContents()))
        .collect(Collectors.toList());

    final List<Variable> unfilteredVars = outputVariables.stream()
        .filter(var -> var.getEntity().equals(currentTraversalNode.getContents()) &&
            applicableFilters.stream().noneMatch(filter -> filter.getEntity().equals(var.getEntity())))
        .collect(Collectors.toList());

    final boolean includeUnfilteredStream = !unfilteredVars.isEmpty() || outputVariables.isEmpty();

    // Convert the entity node to a data flow node.
    final DataFlowNodeContents contents = new DataFlowNodeContents(
        applicableFilters,
        currentTraversalNode.getContents(),
        study,
        includeUnfilteredStream
    );
    TreeNode<DataFlowNodeContents> newRoot = new TreeNode<>(contents);
    Optional<TreeNode<Entity>> parentOpt = parentRetriever.apply(currentTraversalNode);
    // Only add child node if parent exists or parent is not the previous node.
    parentOpt.filter(parent -> parent != previousNode)
        .ifPresent(parent -> newRoot.addChildNode(rerootTree(parentRetriever, parent, currentTraversalNode, filters, outputVariables, study)));
    for (TreeNode<Entity> child : currentTraversalNode.getChildNodes()) {
      // Since we traverse the parent node, ensure we don't traverse backwards.
      if (previousNode != child) {
        newRoot.addChildNode(rerootTree(parentRetriever, child, currentTraversalNode, filters, outputVariables, study));
      }
    }
    return newRoot;
  }
}
