package org.veupathdb.service.eda.subset.model.db;

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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.subset.model.Entity;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.subset.model.db.ResultSetUtils.*;

public class EntityFactory {
  private static final Logger LOG = LogManager.getLogger(EntityFactory.class);
  private final static String STDY_ABBRV_COL_NM = "study_abbrev"; // for private queries

  private final DataSource _dataSource;
  private final String _appDbSchema;
  private final boolean _orderEntities;

  public EntityFactory(DataSource dataSource, String appDbSchema, boolean orderEntities) {
    _dataSource = dataSource;
    _appDbSchema = appDbSchema;
    _orderEntities = orderEntities;
  }

  public TreeNode<Entity> getStudyEntityTree(String studyId) {

    String sql = generateEntityTreeSql(studyId, _appDbSchema, _orderEntities);

    // entityID -> list of child entities
    Map<String, List<Entity>> childrenMap = new HashMap<>();

    Entity rootEntity = new SQLRunner(_dataSource, sql, "Get entity tree").executeQuery(rs -> {
      Entity root = null;
      while (rs.next()) {
        Entity entity = createEntityFromResultSet(rs, _orderEntities);
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

  static String generateEntityTreeSql(String studyId, String appDbSchema, boolean orderEntities) {
    final String sqlOrderByClause = orderEntities
        ? " ORDER BY e." + DB.Tables.EntityTypeGraph.Columns.ENTITY_LOAD_ORDER_ID + " ASC"
        : "";
    final List<String> columns = orderEntities
        ? DB.Tables.EntityTypeGraph.Columns.ALL
        : DB.Tables.EntityTypeGraph.Columns.ALL.stream()
        .filter(c -> !c.equals(DB.Tables.EntityTypeGraph.Columns.ENTITY_LOAD_ORDER_ID))
        .collect(Collectors.toList());
    return "SELECT " +
        "e." + String.join(", e.", columns) + ", " +
        "s." + DB.Tables.Study.Columns.STUDY_ABBREV_COL_NAME + " as " + STDY_ABBRV_COL_NM + NL +
        "FROM " +
        appDbSchema + DB.Tables.EntityTypeGraph.NAME + " e, " +
        appDbSchema + DB.Tables.Study.NAME + " s " +
        "WHERE s." + DB.Tables.Study.Columns.STUDY_ID_COL_NAME + " = '" + studyId + "'" + NL +
        "AND e." + DB.Tables.EntityTypeGraph.Columns.ENTITY_STUDY_ID_COL_NAME + " = s." + DB.Tables.Study.Columns.STUDY_ID_COL_NAME + NL +
        // This ordering ensures the produced tree is displayed in load order;
        //   also stable ordering supports unit testing
        sqlOrderByClause;
  }

  static Entity createEntityFromResultSet(ResultSet rs, boolean orderEntities) {
    try {
      String name = getRsRequiredString(rs, DB.Tables.EntityTypeGraph.Columns.DISPLAY_NAME_COL_NAME);
      // TODO remove this hack when db has plurals
      String namePlural = getRsOptionalString(rs, DB.Tables.EntityTypeGraph.Columns.DISPLAY_NAME_PLURAL_COL_NAME, name + "s");
      String id = getRsRequiredString(rs, DB.Tables.EntityTypeGraph.Columns.ENTITY_ID_COL_NAME);
      String studyAbbrev = getRsRequiredString(rs, STDY_ABBRV_COL_NM);
      String descrip = getRsOptionalString(rs, DB.Tables.EntityTypeGraph.Columns.DESCRIP_COL_NAME, "No Entity Description available");
      String abbrev = getRsRequiredString(rs, DB.Tables.EntityTypeGraph.Columns.ENTITY_ABBREV_COL_NAME);
      long loadOrder = orderEntities
          ? getIntegerFromString(rs, DB.Tables.EntityTypeGraph.Columns.ENTITY_LOAD_ORDER_ID, true)
          : -1L;
      boolean hasCollections = getRsRequiredBoolean(rs, DB.Tables.EntityTypeGraph.Columns.ENTITY_HAS_ATTRIBUTE_COLLECTIONS);

      boolean isManyToOneWithParent = getRsOptionalBoolean(rs, DB.Tables.EntityTypeGraph.Columns.ENTITY_IS_MANY_TO_ONE_WITH_PARENT, true);

      return new Entity(id, studyAbbrev, name, namePlural, descrip, abbrev, loadOrder, hasCollections, isManyToOneWithParent);
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
