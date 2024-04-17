package org.veupathdb.service.eda.subset.model.db;

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
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.varcollection.CollectionType;
import org.veupathdb.service.eda.subset.model.varcollection.DateVarCollection;
import org.veupathdb.service.eda.subset.model.varcollection.FloatingPointVarCollection;
import org.veupathdb.service.eda.subset.model.varcollection.IntegerVarCollection;
import org.veupathdb.service.eda.subset.model.varcollection.VarCollection;
import org.veupathdb.service.eda.subset.model.variable.VariableDataShape;
import org.veupathdb.service.eda.subset.model.varcollection.StringVarCollection;

import static org.veupathdb.service.eda.subset.model.db.VariableFactory.createDateDistributionConfig;
import static org.veupathdb.service.eda.subset.model.db.VariableFactory.createFloatDistributionConfig;
import static org.veupathdb.service.eda.subset.model.db.VariableFactory.createFloatProperties;
import static org.veupathdb.service.eda.subset.model.db.VariableFactory.createIntegerDistributionConfig;
import static org.veupathdb.service.eda.subset.model.db.VariableFactory.createIntegerProperties;

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
        String collectionId = ResultSetUtils.getRsRequiredString(rs, DB.Tables.CollectionAttribute.Columns.COLLECTION_ID);
        String variableId = ResultSetUtils.getRsRequiredString(rs, DB.Tables.CollectionAttribute.Columns.VARIABLE_ID);
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
        ResultSetUtils.getRsRequiredString(rs, DB.Tables.Collection.Columns.COLLECTION_ID),
        ResultSetUtils.getRsRequiredString(rs, DB.Tables.Collection.Columns.DISPLAY_NAME),
        type,
        VariableDataShape.fromString(ResultSetUtils.getRsRequiredString(rs, DB.Tables.Collection.Columns.DATA_SHAPE)),
        ResultSetUtils.getRsRequiredLong(rs, DB.Tables.Collection.Columns.NUM_MEMBERS),
        ResultSetUtils.getRsRequiredBoolean(rs, DB.Tables.Collection.Columns.IMPUTE_ZERO),
        ResultSetUtils.getRsOptionalString(rs, DB.Tables.Collection.Columns.NORMALIZATION_METHOD, null),
        ResultSetUtils.getRsRequiredBoolean(rs, DB.Tables.Collection.Columns.IS_COMPOSITIONAL),
        ResultSetUtils.getRsRequiredBoolean(rs, DB.Tables.Collection.Columns.IS_PROPORTION),
        ResultSetUtils.getRsRequiredString(rs, DB.Tables.Collection.Columns.MEMBER),
        ResultSetUtils.getRsRequiredString(rs, DB.Tables.Collection.Columns.MEMBER_PLURAL)
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
