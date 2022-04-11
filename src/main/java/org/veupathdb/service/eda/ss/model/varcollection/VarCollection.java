package org.veupathdb.service.eda.ss.model.varcollection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.veupathdb.service.eda.generated.model.APICollectionType;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.VariableDataShape;

public abstract class VarCollection {

  public static class Properties {

    public final String id;
    public final String displayName;
    public final APICollectionType type;
    public final VariableDataShape dataShape;
    public final Long numMembers;
    public final Boolean imputeZero;

    public Properties(String id, String displayName,
                      APICollectionType type, VariableDataShape dataShape,
                      Long numMembers, Boolean imputeZero) {
      this.id = id;
      this.displayName = displayName;
      this.type = type;
      this.dataShape = dataShape;
      this.numMembers = numMembers;
      this.imputeZero = imputeZero;
    }
  }

  private final Properties _properties;
  private final List<String> _memberVariableIds = new ArrayList<>();
  private final Set<String> _vocabulary = new HashSet<>();

  protected VarCollection(Properties properties) {
    _properties = properties;
  }

  public void addMemberVariableId(String memberVariableId) {
    _memberVariableIds.add(memberVariableId);
  }

  public void buildAndValidate(Entity entity) {
    // TODO here:
    //  1. populate vocabulary
    //  2. check num members
    //  3. heck for matching types, shapes, etc.
    //  4. assign date bin width and units in distribution subclasses
  }

  public APICollectionType getType() {
    return _properties.type;
  }

  public String getId() {
    return _properties.id;
  }

  public String getDisplayName() {
    return _properties.displayName;
  }

  public VariableDataShape getDataShape() {
    return _properties.dataShape;
  }

  public Boolean getImputeZero() {
    return _properties.imputeZero;
  }

  public Long getDistinctValuesCount() {
    return Integer.valueOf(_vocabulary.size()).longValue();
  }

  public List<String> getMemberVariableIds() {
    return _memberVariableIds;
  }

  public Set<String> getVocabulary() {
    return _vocabulary;
  }
}
