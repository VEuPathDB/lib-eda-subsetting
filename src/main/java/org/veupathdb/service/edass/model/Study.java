package org.veupathdb.service.edass.model;

import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.edass.Resources;
import org.veupathdb.service.edass.generated.model.APIFilter;

import javax.sql.DataSource;
import java.util.*;
import java.util.stream.Collectors;

public class Study {
  private TreeNode<Entity> entityTree;
  private final Map<String, Entity> entityIdMap;
  private final StudyOverview overview;
  
  public Study(StudyOverview overview, TreeNode<Entity> entityTree, Map<String, Entity> entityIdMap) {
    this.entityTree = entityTree;
    this.entityIdMap = entityIdMap;
    this.overview = overview;
    initEntities(entityTree);
  }

  public static List<StudyOverview> getStudyOverviews(DataSource datasource) {
    String sql =
        "select " + RdbmsColumnNames.STUDY_ID_COL_NAME +
                ", " + RdbmsColumnNames.STUDY_ABBREV_COL_NAME +
        " from " + Resources.getAppDbSchema() + RdbmsColumnNames.STUDY_TABLE_NAME;
    return new SQLRunner(datasource, sql).executeQuery(rs -> {
      List<StudyOverview> studyOverviews = new ArrayList<>();
      while (rs.next()) {
        String id = rs.getString(1);
        String abbrev = rs.getString(2);
        StudyOverview study = new StudyOverview(id, abbrev);
        studyOverviews.add(study);
      }
      return studyOverviews;
    });
  }

  /*
   * Expects a pre-validated study ID
   */
  public static Study loadStudy(DataSource datasource, String studyId) {

    StudyOverview overview = getStudyOverview(datasource, studyId);

    TreeNode<Entity> entityTree = EntityResultSetUtils.getStudyEntityTree(datasource, studyId);
    
    Map<String, Entity> entityIdMap = entityTree.flatten().stream().collect(Collectors.toMap(Entity::getId, e -> e));

    for (Entity entity : entityIdMap.values()) entity.loadVariables(datasource);

    return new Study(overview, entityTree, entityIdMap);
  }

  public static StudyOverview getStudyOverview(DataSource datasource, String studyId) {
    String sql =
            "select " + RdbmsColumnNames.STUDY_ID_COL_NAME +
                    ", " + RdbmsColumnNames.STUDY_ABBREV_COL_NAME +
                    " from " + Resources.getAppDbSchema() + RdbmsColumnNames.STUDY_TABLE_NAME +
                     " where " + RdbmsColumnNames.STUDY_ID_COL_NAME + " = '" + studyId + "'";
    return new SQLRunner(datasource, sql).executeQuery(rs -> {
      rs.next();
      String id = rs.getString(1);
      String abbrev = rs.getString(2);
      return new StudyOverview(id, abbrev);
    });
  }

  /**
   * @return list of error messages, if any
   */
  // TODO
  public List<String> validateEntityVariables(String entityId, List<String> variableIds) {
    return null;
  }
  
  /**
   * @return list of error messages, if any
   */
  // TODO
  public List<String> validateApiFilters(List<APIFilter> apiFilters) {
    return null;
  }
  
  /**
   * Build internal (convenience) state from the raw variables set
   */
  void initEntities(TreeNode<Entity> rootEntityNode) {
    entityTree = rootEntityNode;
    validateEntityTreeIds(rootEntityNode);
    
    // give each entity a set of its ancestor entities.
    populateEntityAncestors(rootEntityNode);
  }  

  public String getStudyId() {
    return overview.id;
  }

  public Optional<Entity> getEntity(String entityId) {
    return Optional.ofNullable(entityIdMap.get(entityId));
  }
   
  public TreeNode<Entity> getEntityTree() {
    return entityTree.clone();
  }
  
  private static void populateEntityAncestors(TreeNode<Entity> rootEntityNode) {
    populateEntityAncestors(rootEntityNode, new ArrayList<>());
  }
  
  private static void populateEntityAncestors(TreeNode<Entity> entityNode, List<Entity> ancestorEntities) {
    Entity entity = entityNode.getContents();
    entity.setAncestorEntities(ancestorEntities);
    ancestorEntities.add(entity);
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


  /* a brief version of the study */
  public static class StudyOverview {
    private final String id;
    private final String internalAbbrev;

    public StudyOverview(String id, String internalAbbrev) {
      this.id = id;
      this.internalAbbrev = internalAbbrev;
    }

    public String getId() {
      return id;
    }

    public String getInternalAbbrev() {
      return internalAbbrev;
    }
  }


}
