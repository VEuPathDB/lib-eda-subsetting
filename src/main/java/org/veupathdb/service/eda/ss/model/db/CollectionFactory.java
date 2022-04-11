package org.veupathdb.service.eda.ss.model.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.Functions;
import org.veupathdb.service.eda.generated.model.APICollectionType;
import org.veupathdb.service.eda.ss.Resources;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.varcollection.DateVarCollection;
import org.veupathdb.service.eda.ss.model.varcollection.FloatingPointVarCollection;
import org.veupathdb.service.eda.ss.model.varcollection.IntegerVarCollection;
import org.veupathdb.service.eda.ss.model.varcollection.VarCollection;
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

  private final DataSource _dataSource;

  public CollectionFactory(DataSource dataSource) {
    _dataSource = dataSource;
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

    // assign variable to its collection
    //collectionMap.get(collectionId).addMemberVariableId("");

  }

  private Map<String, VarCollection> loadCollectionMap(Entity entity) {
    // build map of collections for this entity
    String collectionSql =
        "select " + DB.Tables.Collection.Columns.ALL.stream().collect(Collectors.joining(", ")) +
        " from " + Resources.getAppDbSchema() + DB.Tables.Collection.NAME(entity);
    return new SQLRunner(_dataSource, collectionSql, "select-collection").executeQuery(rs -> {
      Map<String, VarCollection> map = new HashMap<>();
      while (rs.next()) {
        VarCollection collection = loadCollection(rs);
        map.put(collection.getId(), collection);
      }
      return map;
    });
  }

  private VarCollection loadCollection(ResultSet rs) throws SQLException {

    // find data type of this collection
    APICollectionType type = Functions.mapException(() ->
        APICollectionType.valueOf(rs.getString(DB.Tables.Collection.Columns.DATA_TYPE).toUpperCase()),
        e -> new RuntimeException("Invalid collection data type", e));

    // load properties common to all types
    VarCollection.Properties properties = new VarCollection.Properties(
        getRsRequiredString(rs, COLLECTION_ID),
        getRsRequiredString(rs, DISPLAY_NAME),
        type,
        VariableDataShape.fromString(getRsRequiredString(rs, DATA_SHAPE)),
        getRsRequiredLong(rs, NUM_MEMBERS),
        getRsRequiredBoolean(rs, IMPUTE_ZERO)
    );

    // create typed collection, loading type-specific props
    return switch (type) {
      case DATE -> new DateVarCollection(properties, createDateDistributionConfig(properties.dataShape, rs, false));
      case INTEGER -> new IntegerVarCollection(properties, createIntegerProperties(rs), createIntegerDistributionConfig(rs, false));
      case NUMBER -> new FloatingPointVarCollection(properties, createFloatProperties(rs), createFloatDistributionConfig(rs, false));
    };
  }
}
