package org.veupathdb.service.eda.subset.model.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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

  // Cache the DB platform, it is set at startup and will never change.
  private static PlatformUtils.DBPlatform _dbPlatform = null;

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
        boolean attributesTableExists = attributesTableExists(entity);
        entity.setAttributesTableExists(attributesTableExists);
        String parentId = rs.getString(DB.Tables.EntityTypeGraph.Columns.ENTITY_PARENT_ID_COL_NAME);
        if (parentId == null || parentId.isEmpty()) {
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

  /**
   * Checks if the ATTRIBUTES table exists for a given entity. This "wide" table contains rows with every variable
   * value. This table's structure is required for pagination/sorting of the data. The existence of this indicates
   * to clients whether certain functionality (e.g. previewing downloads) is available for this entity.
   */
  private boolean attributesTableExists(Entity entity) {
    if (_dbPlatform == null) {
      _dbPlatform = PlatformUtils.fromDataSource(_dataSource);
    }
    String wideTable = DB.Tables.Attributes.NAME(entity).toUpperCase(Locale.ROOT);
    if (_dbPlatform == PlatformUtils.DBPlatform.PostgresDB) {
      String postgresTableExists = String.format("SELECT EXISTS (\n" +
          "   SELECT 1\n" +
          "   FROM information_schema.tables\n" +
          "   WHERE table_schema = '%s'\n" +
          "   AND table_name = '%s'\n" +
          ");", _appDbSchema, wideTable);
      return new SQLRunner(_dataSource, postgresTableExists).executeQuery(ResultSet::next);
    }
    String tableExistsSql = String.format("SELECT object_name FROM all_objects WHERE object_name = '%s'", wideTable);
    return new SQLRunner(_dataSource, tableExistsSql).executeQuery(ResultSet::next);
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
        ? " ORDER BY e." + DB.Tables.EntityTypeGraph.Columns.DISPLAY_NAME_COL_NAME + " ASC"
        : "";
    return "SELECT " +
        "e." + String.join(", e.", DB.Tables.EntityTypeGraph.Columns.ALL) + ", " +
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
      boolean hasCollections = getRsRequiredBoolean(rs, DB.Tables.EntityTypeGraph.Columns.ENTITY_HAS_ATTRIBUTE_COLLECTIONS);
      boolean isManyToOneWithParent = getRsOptionalBoolean(rs, DB.Tables.EntityTypeGraph.Columns.ENTITY_IS_MANY_TO_ONE_WITH_PARENT, true);
      return new Entity(id, studyAbbrev, name, namePlural, descrip, abbrev, hasCollections, isManyToOneWithParent);
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
