package org.veupathdb.service.eda.subset.testutil;

import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.StudyOverview;
import org.veupathdb.service.eda.subset.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.subset.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.subset.model.variable.DateVariable;
import org.veupathdb.service.eda.subset.model.variable.IntegerVariable;
import org.veupathdb.service.eda.subset.model.variable.StringVariable;
import org.veupathdb.service.eda.subset.model.variable.Variable;
import org.veupathdb.service.eda.subset.model.variable.VariableDataShape;
import org.veupathdb.service.eda.subset.model.variable.VariableType;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TestDataProvider {
  public static final String ENTITY_ID = "EUPA_1024";
  public static final String STUDY_ID = "GEMS1A";
  public static final String VARIABLE_ID = "EUPA_1111";

  public static Entity constructEntity() {
    return new Entity(
        ENTITY_ID,
        STUDY_ID,
        "My Study",
        "My Studies",
        "My favority study",
        "Mine",
        0L,
        false,
        false);
  }

  public static IntegerVariable constructIntVariable(Entity entity) {
    return new IntegerVariable(
        constructGenericVarProps(entity, VARIABLE_ID),
        constructVarValuesProps(VariableType.INTEGER, false),
        new NumberDistributionConfig<>(0L, 10L, 0L, 10L, 2L, 2L),
        new IntegerVariable.Properties("bleep bloops")
    );
  }

  public static DateVariable constructDateVariable(Entity entity) {
    return new DateVariable(
        constructGenericVarProps(entity, VARIABLE_ID),
        constructVarValuesProps(VariableType.DATE, false),
        new DateDistributionConfig(false, VariableDataShape.CONTINUOUS, null, null, "min", "max", 10, "days", null)
    );
  }

  public static class StudyBuilder {
    private String studyId;
    private String internalAbbrev;
    private TreeNode<Entity> rootEntity;
    private Map<String, Entity> entityIdMap = new HashMap<>();

    public StudyBuilder(String studyId, String internalAbbrev) {
      this.studyId = studyId;
      this.internalAbbrev = internalAbbrev;
    }

    public StudyBuilder withRoot(Entity entity) {
      entityIdMap.put(entity.getId(), entity);
      this.rootEntity = new TreeNode<>(entity);
      return this;
    }

    public StudyBuilder addEntity(Entity entity, String parentId) {
      TreeNode<Entity> parent = rootEntity.findFirst(e -> e.getId().equals(parentId));
      entityIdMap.put(entity.getId(), entity);
      parent.addChild(entity);
      return this;
    }

    public Study build() {
      final StudyOverview studyOverview = new StudyOverview(studyId, internalAbbrev, StudyOverview.StudySourceType.CURATED, new Date());
      return new Study(studyOverview, rootEntity, entityIdMap);
    }

  }

  public static class EntityBuilder {
    private String entityId;
    private String displayName;
    private String internalStudyAbbrev;
    private String description;
    private String abbreviation;
    private boolean isManyToOneWithParent;

    public EntityBuilder withEntityId(String entityId) {
      this.entityId = entityId;
      return this;
    }

    public EntityBuilder withInternalStudyAbbrev(String internalStudyAbbrev) {
      this.internalStudyAbbrev = internalStudyAbbrev;
      return this;
    }

    public EntityBuilder withDisplayName(String displayName) {
      this.displayName = displayName;
      return this;
    }

    public Entity build() {
      return new Entity(
          entityId,
          internalStudyAbbrev,
          displayName,
          displayName + "s",
          description,
          "Mine",
          0L,
          false,
          true);

    }
  }

  public static class DateVariableBuilder {
    private VariableType variableType;
    private String variableId;
    private Entity entity;

    public DateVariableBuilder withVariableId(String variableId) {
      this.variableId = variableId;
      return this;
    }

    public DateVariableBuilder withEntity(Entity entity) {
      this.entity = entity;
      return this;
    }

    public DateVariableBuilder withVariableType(VariableType variableType) {
      this.variableType = variableType;
      return this;
    }

    public DateVariable build() {
      return new DateVariable(
          constructGenericVarProps(entity, variableId),
          constructVarValuesProps(variableType, false),
          new DateDistributionConfig(false, VariableDataShape.CONTINUOUS, "", "", "", "", 5, "day", "day")
      );
    }
  }

  public static class StringVariableBuilder {
    private Entity entity;
    private String variableId;
    private VariableType variableType;
    private int maxLength = 200;

    public StringVariableBuilder withEntity(Entity entity) {
      this.entity = entity;
      return this;
    }

    public StringVariableBuilder withVariableId(String variableId) {
      this.variableId = variableId;
      return this;
    }

    public StringVariableBuilder withVariableType(VariableType variableType) {
      this.variableType = variableType;
      return this;
    }

    public StringVariableBuilder withMaxLength(int maxLength) {
      this.maxLength = maxLength;
      return this;
    }

    public StringVariable build() {
      return new StringVariable(
          constructGenericVarProps(entity, variableId),
          constructVarValuesProps(variableType, false),
          new StringVariable.StringBinaryProperties(maxLength)
      );
    }
  }

  public static class IntegerVariableBuilder {
    private VariableType variableType;
    private String variableId;
    private Entity entity;
    private long max;
    private long min;
    private boolean multiValued = false;

    public IntegerVariableBuilder withVariableId(String variableId) {
      this.variableId = variableId;
       return this;
    }

    public IntegerVariableBuilder withEntity(Entity entity) {
      this.entity = entity;
      return this;
    }

    public IntegerVariableBuilder withVariableType(VariableType variableType) {
      this.variableType = variableType;
      return this;
    }

    public IntegerVariableBuilder withMultiValued(boolean multiValued) {
      this.multiValued = multiValued;
      return this;
    }

    public IntegerVariableBuilder withMax(long max) {
      this.max = max;
      return this;
    }

    public IntegerVariableBuilder withMin(long min) {
      this.min = min;
      return this;
    }

    public IntegerVariable build() {
      return new IntegerVariable(
          constructGenericVarProps(entity, variableId),
          constructVarValuesProps(variableType, multiValued),
          new NumberDistributionConfig<>(min, max, min, max, 2L, 2L),
          new IntegerVariable.Properties("bleep bloops")
      );
    }
  }

  private static Variable.Properties constructGenericVarProps(Entity entity, String variableId) {
    return new Variable.Properties(
        "label",
        variableId,
        entity,
        null,
        null,
        0L,
        "50",
        null,
        null
    );
  }

  private static VariableWithValues.Properties constructVarValuesProps(VariableType variableType, boolean multiValued) {
    return new VariableWithValues.Properties(
        variableType,
        null,
        Collections.emptyList(),
        0L,
        false,
        false,
        false,
        multiValued,
        false,
        false,
        null);
  }
}
