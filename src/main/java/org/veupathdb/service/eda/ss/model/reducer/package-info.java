/**
 * This package enables a reduction of a hierarchical entity tree into a tabular subsetting result. The entry point
 * into this class is the {@link org.veupathdb.service.eda.ss.model.reducer.FileBasedTabularSubsetter} which is
 * responsible for constructing the tree, and initiating its traversal. See (TODO TO BE IMPLEMENTED)
 * for details on the construction of the tree and {@link org.veupathdb.service.eda.ss.model.reducer.EntityJoinerRoot}
 * for details on its traversal.
 *
 * The tree is comprised of a root {@link org.veupathdb.service.eda.ss.model.reducer.SubsettingJoinNode} which has
 * N child nodes. Each node corresponds to an entity for which our subsetting query requires us to either filter
 * data points or output data points in our tabular result. The nodes open binary-encoded files, each file
 * corresponding to a particular variable, in order to filter and output data. These datastreams are joined by
 * the {@link org.veupathdb.service.eda.ss.model.reducer.StreamIntersectMerger} utility class which takes
 * N sorted sets of data and outputs a sorted stream representing the intersection of the sets. Note that as data
 * flows toward the root of the tree, an {@link org.veupathdb.service.eda.ss.model.reducer.ancestor.AncestorExpander}
 * or {@link org.veupathdb.service.eda.ss.model.reducer.ancestor.DescendantCollapser} is used to map the ID indexes
 * output representing the child node's entity to the ID index of the entity represented by the parent node.
 */
package org.veupathdb.service.eda.ss.model.reducer;