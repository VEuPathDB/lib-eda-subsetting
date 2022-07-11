package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.reducer.ancestor.AncestorMapperFactory;
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
public class EntityJoinerRoot {
  private final List<Iterator<VariableValueIdPair<?>>> valueStreams;
  private final SubsettingJoinNode rootNode;

  public EntityJoinerRoot(List<Iterator<Long>> filteredStreams,
                          List<Iterator<VariableValueIdPair<?>>> valueStreams,
                          List<SubsettingJoinNode> children,
                          Entity entity,
                          Study study,
                          AncestorMapperFactory ancestorMapperFactory) {
    this.valueStreams = valueStreams;
    this.rootNode = new SubsettingJoinNode(filteredStreams, children, entity, study, ancestorMapperFactory);
  }

  /**
   * Merge the filtered idIndex streams and map them to the values provided in the {@link EntityJoinerRoot#valueStreams}.
   * @return
   */
  public Iterator<List<String>> reduce() {
    return new ValueExtractor(valueStreams, rootNode.reduce());
  }
}
