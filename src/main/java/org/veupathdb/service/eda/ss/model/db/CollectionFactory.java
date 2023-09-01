package org.veupathdb.service.eda.ss.model.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.Functions;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.varcollection.*;
import org.veupathdb.service.eda.ss.model.variable.VariableDataShape;

import static org.veupathdb.service.eda.ss.model.db.DB.Tables.Collection.Columns.*;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getRsRequiredBoolean;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getRsRequiredLong;
import static org.veupathdb.service.eda.ss.model.db.ResultSetUtils.getRsRequiredString;
import static org.veupathdb.service.eda.ss.model.db.VariableFactory.createDateDistributionConfig;
import static org.veupathdb.service.eda.ss.model.db.VariableFactory.createFloatDistributionConfig;
import static org.veupathdb.service.eda.ss.model.db.VariableFactory.createFloatProperties;
import static org.veupathdb.service.eda.ss.model.db.VariableFactory.createIntegerDistributionConfig;
import static org.veupathdb.service.eda.ss.model.db.VariableFactory.createIntegerProperties;

public class CollectionFactory {

  private static final Logger LOG = LogManager.getLogger(CollectionFactory.class);

  private final DataSource _dataSource;
  private final String _appDbSchema;

  public CollectionFactory(DataSource dataSource, String appDbSchema) {
    _dataSource = dataSource;
    _appDbSchema = appDbSchema;
  }

  public List<VarCollection> loadCollections(Entity entity) {

    // load base collection metadata (does not include member vars)
    Map<String, VarCollection> collectionMap = loadCollectionMap(entity);

    // populate with member vars
    assignMemberVariables(entity, collectionMap);

    // complete processing for each collection
    collectionMap.values().stream().forEach(c -> c.buildAndValidate(entity));

    // convert to returnable list
    return new ArrayList<>(collectionMap.values());

  }

  private void assignMemberVariables(Entity entity, Map<String, VarCollection> collectionMap) {
    String sql =
        "select " + String.join(", ", DB.Tables.CollectionAttribute.Columns.ALL) +
        " from " + _appDbSchema + DB.Tables.CollectionAttribute.NAME(entity);
    new SQLRunner(_dataSource, sql, "select-collection-vars").executeQuery(rs -> {
      while (rs.next()) {
        // assign variable to its collection
        String collectionId = getRsRequiredString(rs, DB.Tables.CollectionAttribute.Columns.COLLECTION_ID);
        String variableId = getRsRequiredString(rs, DB.Tables.CollectionAttribute.Columns.VARIABLE_ID);
        collectionMap.get(collectionId).addMemberVariableId(variableId);
      }
      return null;
    });
  }

  private Map<String, VarCollection> loadCollectionMap(Entity entity) {
    String sql =
        "select " + String.join(", ", DB.Tables.Collection.Columns.ALL) +
        " from " + _appDbSchema + DB.Tables.Collection.NAME(entity);
    return new SQLRunner(_dataSource, sql, "select-collection").executeQuery(rs -> {
      // build map of collections for this entity
      Map<String, VarCollection> map = new HashMap<>();
      while (rs.next()) {
        VarCollection collection = loadCollection(rs);
        map.put(collection.getId(), collection);
      }
      LOG.info("Loaded metadata for " + map.size() + " collections on entity " + entity.getId());
      return map;
    });
  }

  private static VarCollection loadCollection(ResultSet rs) throws SQLException {

    // find data type of this collection
    CollectionType type = Functions.mapException(() ->
        CollectionType.valueOf(rs.getString(DB.Tables.Collection.Columns.DATA_TYPE).toUpperCase()),
        e -> new RuntimeException("Invalid collection data type", e));

    // load properties common to all types
    VarCollection.Properties properties = new VarCollection.Properties(
        getRsRequiredString(rs, COLLECTION_ID),
        getRsRequiredString(rs, DISPLAY_NAME),
        type,
        VariableDataShape.fromString(getRsRequiredString(rs, DATA_SHAPE)),
        getRsRequiredLong(rs, NUM_MEMBERS),
        getRsRequiredBoolean(rs, IMPUTE_ZERO),
        getRsRequiredString(rs, NORMALIZATION_METHOD),
        getRsRequiredBoolean(rs, IS_COMPOSITIONAL),
        getRsRequiredBoolean(rs, IS_PROPORTION)
        );

    // create typed collection, loading type-specific props
    switch (type) {
      case DATE: return new DateVarCollection(properties, createDateDistributionConfig(properties.dataShape, rs, false));
      case INTEGER: return new IntegerVarCollection(properties, createIntegerProperties(rs), createIntegerDistributionConfig(rs, false));
      case NUMBER: return new FloatingPointVarCollection(properties, createFloatProperties(rs, false), createFloatDistributionConfig(rs, false));
      case STRING: return new StringVarCollection(properties);
      default: throw new IllegalArgumentException();
    }
  }
}
