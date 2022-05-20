package org.veupathdb.service.eda.ss.model.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.ss.Resources;
import org.veupathdb.service.eda.ss.model.Entity;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.EntityTypeGraph.Columns.*;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getIntegerFromString;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getRsOptionalString;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getRsRequiredBoolean;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getRsRequiredString;

public class EntityFactory {

  private final static String STDY_ABBRV_COL_NM = "study_abbrev"; // for private queries

  private final DataSource _dataSource;

  public EntityFactory(DataSource dataSource) {
    _dataSource = dataSource;
  }

  public TreeNode<Entity> getStudyEntityTree(String studyId) {

    String sql = generateEntityTreeSql(studyId);

    // entityID -> list of child entities
    Map<String, List<Entity>> childrenMap = new HashMap<>();

    Entity rootEntity = new SQLRunner(_dataSource, sql, "Get entity tree").executeQuery(rs -> {
      Entity root = null;
      while (rs.next()) {
        Entity entity = createEntityFromResultSet(rs);
        String parentId = rs.getString(DB.Tables.EntityTypeGraph.Columns.ENTITY_PARENT_ID_COL_NAME);
        if (parentId == null) {
          if (root != null) throw new RuntimeException("In Study " + studyId +
              " found more than one root entity (" + root.getId() + ", " + entity.getId() + ")");
          root = entity;
        }
        else {
          if (!childrenMap.containsKey(parentId)) childrenMap.put(parentId, new ArrayList<>());
          childrenMap.get(parentId).add(entity);
        }
      }
      return root;
    });

    if (rootEntity == null)
      throw new RuntimeException("Found no entities for study: " + studyId);

    return generateEntityTree(rootEntity, childrenMap);
  }

  private static TreeNode<Entity> generateEntityTree(Entity rootEntity, Map<String, List<Entity>> childrenMap) {
    // create a new node for this entity
    TreeNode<Entity> rootNode = new TreeNode<>(rootEntity);
    // create subtree nodes for all children and add
    rootNode.addAllChildNodes(
        // get the children of this node
        Optional.ofNullable(childrenMap.get(rootEntity.getId()))
            // if no children added, use empty list
            .orElse(Collections.emptyList()).stream()
            // map each child to a tree
            .map(child -> generateEntityTree(child, childrenMap))
            // collect children into a list
            .collect(Collectors.toList())
    );
    return rootNode;
  }

  static String generateEntityTreeSql(String studyId) {
    return "SELECT " +
        "e2." + DB.Tables.EntityType.Columns.ISA_TYPE + ", " +
        "e." + String.join(", e.", DB.Tables.EntityTypeGraph.Columns.ALL) + ", " +
        "s." + DB.Tables.Study.Columns.STUDY_ABBREV_COL_NAME + " as " + STDY_ABBRV_COL_NM + NL +
        "FROM " +
        Resources.getAppDbSchema() + DB.Tables.EntityTypeGraph.NAME + " e, " +
        Resources.getAppDbSchema() + DB.Tables.Study.NAME + " s, " +
        Resources.getAppDbSchema() + DB.Tables.EntityType.NAME + " e2 " + NL +
        "WHERE s." + DB.Tables.Study.Columns.STUDY_ID_COL_NAME + " = '" + studyId + "'" + NL +
        "AND e." + DB.Tables.EntityTypeGraph.Columns.ENTITY_STUDY_ID_COL_NAME + " = s." + DB.Tables.Study.Columns.STUDY_ID_COL_NAME + NL +
        "AND e." + DB.Tables.EntityTypeGraph.Columns.ENTITY_LOAD_ORDER_ID + " = e2." + DB.Tables.EntityType.Columns.ENTITY_TYPE_ID + NL +
        // This ordering ensures the produced tree is displayed in load order;
        //   also stable ordering supports unit testing
        "ORDER BY e." + DB.Tables.EntityTypeGraph.Columns.ENTITY_LOAD_ORDER_ID + " ASC";
  }

  static Entity createEntityFromResultSet(ResultSet rs) {
    try {
      String name = getRsRequiredString(rs, DISPLAY_NAME_COL_NAME);
      // TODO remove this hack when db has plurals
      String namePlural = getRsOptionalString(rs, DISPLAY_NAME_PLURAL_COL_NAME, name + "s");
      String id = getRsRequiredString(rs, ENTITY_ID_COL_NAME);
      String studyAbbrev = getRsRequiredString(rs, STDY_ABBRV_COL_NM);
      String descrip = getRsOptionalString(rs, DESCRIP_COL_NAME, "No Entity Description available");
      String abbrev = getRsRequiredString(rs, ENTITY_ABBREV_COL_NAME);
      long loadOrder = getIntegerFromString(rs, ENTITY_LOAD_ORDER_ID, true);
      boolean hasCollections = determineHasCollections(getRsRequiredString(rs, DB.Tables.EntityType.Columns.ISA_TYPE)); // getRsRequiredBoolean(rs, ENTITY_HAS_ATTRIBUTE_COLLECTIONS);
      boolean isManyToOneWithParent = false; //getRsRequiredBoolean(rs, ENTITY_IS_MANY_TO_ONE_WITH_PARENT);

      return new Entity(id, studyAbbrev, name, namePlural, descrip, abbrev, loadOrder, hasCollections, isManyToOneWithParent);
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean determineHasCollections(String isaEntityType) {
    return Resources.getConvertAssaysFlag() && isaEntityType.equals("Assay");
  }
}
