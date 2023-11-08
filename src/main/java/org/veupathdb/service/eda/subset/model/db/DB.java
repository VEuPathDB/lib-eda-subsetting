package org.veupathdb.service.eda.subset.model.db;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gusdb.fgputil.functional.Functions;
import org.veupathdb.service.eda.subset.model.Entity;

import static org.veupathdb.service.eda.subset.model.db.DB.ColumnCollection.getAll;
import static org.veupathdb.service.eda.subset.model.db.DB.ColumnCollection.getAllAsStream;

public interface DB {

  interface Tables {

    interface Study {
      String NAME = "study";
      interface Columns extends ColumnCollection {
        String STUDY_ID_COL_NAME = "stable_id";
        String STUDY_ABBREV_COL_NAME = "internal_abbrev";
        String STUDY_DATE_MODIFIED_COL_NAME = "modification_date";
      }
    }

    interface Ancestors {
      String NAME_PREFIX = "ancestors";
      static String NAME(Entity e) { return applyPrefix(NAME_PREFIX, e); }
      interface Columns extends ColumnCollection {
        // table contains columns for the ID of each record and its ancestor records
      }
    }

    interface AttributeGraph {
      String NAME_PREFIX = "attributegraph";
      static String NAME(Entity e) { return applyPrefix(NAME_PREFIX, e); }
      interface Columns extends ColumnCollection {
        String VARIABLE_ID_COL_NAME = "stable_id";
        String VARIABLE_PARENT_ID_COL_NAME = "parent_stable_id";
        String PROVIDER_LABEL_COL_NAME = "provider_label";
        String DISPLAY_NAME_COL_NAME = "display_name";
        String DEFINITION_COL_NAME = "definition";
        String VOCABULARY_COL_NAME = "vocabulary";
        String DISPLAY_TYPE_COL_NAME = "display_type";
        String HIDE_FROM_COL_NAME = "hidden";
        String DISPLAY_ORDER_COL_NAME = "display_order";
        String DISPLAY_RANGE_MIN_COL_NAME = "display_range_min";
        String DISPLAY_RANGE_MAX_COL_NAME = "display_range_max";
        String RANGE_MIN_COL_NAME = "range_min";
        String RANGE_MAX_COL_NAME = "range_max";
        String BIN_WIDTH_OVERRIDE_COL_NAME = "bin_width_override";
        String BIN_WIDTH_COMPUTED_COL_NAME = "bin_width_computed";
        //String MEAN = "mean";
        //String MEDIAN = "median";
        //String LOWER_QUARTILE = "lower_quartile";
        //String UPPER_QUARTILE = "upper_quartile";
        String IS_TEMPORAL_COL_NAME = "is_temporal";
        String IS_FEATURED_COL_NAME = "is_featured";
        String IS_MERGE_KEY_COL_NAME = "is_merge_key";
        String IMPUTE_ZERO = "impute_zero";
        String IS_REPEATED_COL_NAME = "is_repeated";
        String HAS_VALUES_COL_NAME = "has_values";
        String VARIABLE_TYPE_COL_NAME = "data_type";
        String DISTINCT_VALUES_COUNT_COL_NAME = "distinct_values_count";
        String IS_MULTI_VALUED_COL_NAME = "is_multi_valued";
        String DATA_SHAPE_COL_NAME = "data_shape";
        String UNITS_COL_NAME = "unit";
        String PRECISION_COL_NAME = "precision";
        String SCALE_COL_NAME = "scale";
        String HAS_STUDY_DEPENDENT_VOCABULARY = "has_study_dependent_vocabulary";
        String VARIABLE_SPEC_TO_IMPUTE_ZEROES_FOR = "variable_spec_to_impute_zeroes_for";
        List<String> ALL = getAll(Columns.class);
      }
    }

    /**
     * This is the attributes "wide table", having a row for each record and column for each attribute
     */
    interface Attributes {
       String NAME_PREFIX = "attributes";
      static String NAME(Entity e) { return applyPrefix(NAME_PREFIX, e); }
       interface Columns {
         // stable ID is the ID of the row (specific to entity, e.g. household_stable_id)
         String ID = "stable_id";
         // table contains additional columns for each attribute
       }
    }

    /**
     * This is the attributes "tall table", having a row for each record-attribute combination
     */
    interface AttributeValue {
      String NAME_PREFIX = "attributevalue";
      static String NAME(Entity e) { return applyPrefix(NAME_PREFIX, e); }
      interface Columns {
        String TT_VARIABLE_ID_COL_NAME = "attribute_stable_id";
        String STRING_VALUE_COL_NAME = "string_value";
        String DATE_VALUE_COL_NAME = "date_value";
        String NUMBER_VALUE_COL_NAME = "number_value";
      }
    }

    interface Collection {
      String NAME_PREFIX = "collection";
      static String NAME(Entity e) { return applyPrefix(NAME_PREFIX, e); }
      interface Columns extends ColumnCollection {
        String COLLECTION_ID = "stable_id";
        String DISPLAY_NAME = "display_name";
        String NUM_MEMBERS = "num_members";
        String RANGE_MIN = "range_min";
        String RANGE_MAX = "range_max";
        String DISPLAY_RANGE_MIN = "display_range_min";
        String DISPLAY_RANGE_MAX = "display_range_max";
        String IMPUTE_ZERO = "impute_zero";
        String DATA_TYPE = "data_type";
        String DATA_SHAPE = "data_shape";
        String UNIT = "unit";
        String PRECISION = "precision";
        String IS_PROPORTION = "is_proportion";
        String IS_COMPOSITIONAL = "is_compositional";
        String NORMALIZATION_METHOD = "normalization_method";
        List<String> ALL = getAll(Columns.class);
      }
    }

    interface CollectionAttribute {
      String NAME_PREFIX = "collectionattribute";
      static String NAME(Entity e) { return applyPrefix(NAME_PREFIX, e); }
      interface Columns extends ColumnCollection {
        String COLLECTION_ID = "collection_stable_id";
        String VARIABLE_ID = "attribute_stable_id";
        List<String> ALL = getAll(Columns.class);
      }
    }

    interface EntityType {
      String NAME = "EntityType";
      interface Columns extends ColumnCollection {
        String ENTITY_TYPE_ID = "entity_type_id";
        String ISA_TYPE = "isa_type";
      }
    }

    interface EntityTypeGraph {
      String NAME = "EntityTypeGraph";
      interface Columns extends ColumnCollection {
        String ENTITY_ID_COL_NAME = "stable_id";
        String STUDY_ID_COL_NAME = "stable_id";
        String ENTITY_STUDY_ID_COL_NAME = "study_stable_id";
        String ENTITY_PARENT_ID_COL_NAME = "parent_stable_id";
        String ENTITY_ABBREV_COL_NAME = "internal_abbrev";
        String ENTITY_LOAD_ORDER_ID = "entity_type_id";
        String DESCRIP_COL_NAME = "description";
        String DISPLAY_NAME_COL_NAME = "display_name";
        String DISPLAY_NAME_PLURAL_COL_NAME = "display_name_plural";
        String ENTITY_HAS_ATTRIBUTE_COLLECTIONS = "has_attribute_collections";
        String ENTITY_IS_MANY_TO_ONE_WITH_PARENT = "is_many_to_one_with_parent";
        List<String> ALL = getAll(Columns.class);
      }
    }
  }

  static String applyPrefix(String prefix, Entity e) {
    return prefix + "_" + e.getStudyAbbrev() + "_" + e.getAbbreviation();
  }

  interface ColumnCollection {
    static Stream<String> getAllAsStream(Class<?> clazz) {
      return Arrays.stream(clazz.getDeclaredFields())
          .filter(field -> Modifier.isStatic(field.getModifiers()))
          .filter(field -> field.getType().equals(String.class))
          .map(field -> (String)Functions.swallowAndGet(() -> field.get(null)));
    }
    static List<String> getAll(Class<?> clazz) {
      return getAllAsStream(clazz).collect(Collectors.toList());
    }
  }
}
