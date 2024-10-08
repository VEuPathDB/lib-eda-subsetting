package org.veupathdb.service.eda.subset.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.gusdb.fgputil.functional.TreeNode;

public class Study extends StudyOverview {

  private TreeNode<Entity> _entityTree;
  private final Map<String, Entity> _entityIdMap;

  public Study(StudyOverview overview, TreeNode<Entity> entityTree, Map<String, Entity> entityIdMap) {
    super(overview.getStudyId(), overview.getInternalAbbrev(), overview.getStudySourceType(), overview.getLastModified());
    _entityTree = entityTree;
    _entityIdMap = entityIdMap;
    initEntities(entityTree);
  }

  /**
   * Build internal (convenience) state from the raw variables set
   */
  void initEntities(TreeNode<Entity> rootEntityNode) {
    _entityTree = rootEntityNode;
    validateEntityTreeIds(rootEntityNode);

    // give each entity a set of its ancestor entities.
    populateEntityAncestors(rootEntityNode);
  }

  public Optional<Entity> getEntity(String entityId) {
    return Optional.ofNullable(_entityIdMap.get(entityId));
  }

  public TreeNode<Entity> getEntityTree() {
    return _entityTree.clone();
  }

  public boolean hasGeographicData() {
    return _entityTree.reduce(x -> true, (result, entity) -> result || entity.hasGeographicData(), false);
  }

  private static void populateEntityAncestors(TreeNode<Entity> rootEntityNode) {
    populateEntityAncestors(rootEntityNode, new ArrayList<>());
  }

  private static void populateEntityAncestors(TreeNode<Entity> entityNode, List<Entity> ancestorEntities) {
    Entity entity = entityNode.getContents();
    entity.setAncestorEntities(ancestorEntities);
    ancestorEntities.addFirst(entity);
    for (TreeNode<Entity> childNode : entityNode.getChildNodes()) {
      populateEntityAncestors(childNode, new ArrayList<>(ancestorEntities));
    }
  }

  /*
   * Confirm that children have non-conflicting entity IDs. Throw runtime exception if invalid
   */
  private static void validateEntityTreeIds(TreeNode<Entity> entityNode) {
    String errPrefix = "In entity " + entityNode.getContents().getId() +
        ", found a child with the same ID as ";

    Set<String> childEntityIds = new HashSet<>();
    for (TreeNode<Entity> child : entityNode.getChildNodes()) {
      Entity childEntity = child.getContents();
      if (childEntity.getId().equals(entityNode.getContents().getId()))
        throw new RuntimeException(errPrefix + "the parent: " + childEntity.getId());

      if (childEntityIds.contains(childEntity.getId()))
        throw new RuntimeException(errPrefix + "another child: " + childEntity.getId());
      childEntityIds.add(childEntity.getId());
    }
  }
}
