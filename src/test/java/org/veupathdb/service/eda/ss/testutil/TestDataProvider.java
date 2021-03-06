package org.veupathdb.service.eda.ss.testutil;

import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.StudyOverview;
import org.veupathdb.service.eda.ss.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.*;

import java.util.Collections;
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
        constructVarValuesProps(VariableType.INTEGER),
        new NumberDistributionConfig<>(0L, 10L, 0L, 10L, 2L, 2L),
        new IntegerVariable.Properties("bleep bloops")
    );
  }

  public static DateVariable constructDateVariable(Entity entity) {
    return new DateVariable(
        constructGenericVarProps(entity, VARIABLE_ID),
        constructVarValuesProps(VariableType.DATE),
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
      final StudyOverview studyOverview = new StudyOverview(studyId, internalAbbrev, StudyOverview.StudySourceType.CURATED);
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

  public static class IntegerVariableBuilder {
    private VariableType variableType;
    private String variableId;
    private Entity entity;

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

    public IntegerVariable build() {
      return new IntegerVariable(
          constructGenericVarProps(entity, variableId),
          constructVarValuesProps(variableType),
          new NumberDistributionConfig<>(0L, 10L, 0L, 10L, 2L, 2L),
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

  private static VariableWithValues.Properties constructVarValuesProps(VariableType variableType) {
    return new VariableWithValues.Properties(
        variableType,
        null,
        Collections.emptyList(),
        0L,
        false,
        false,
        false,
        false,
        false
    );
  }
}
