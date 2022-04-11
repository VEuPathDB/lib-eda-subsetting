package org.veupathdb.service.eda.ss.model.varcollection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.veupathdb.service.eda.generated.model.APICollectionType;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableDataShape;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

import static org.veupathdb.service.eda.ss.model.db.DB.Tables.Collection.Columns.NUM_MEMBERS;

public abstract class VarCollection {

  private static final Logger LOG = LogManager.getLogger(VarCollection.class);

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

  protected abstract void assignBinValues(List<VariableWithValues> memberVars);

  private final Properties _properties;
  private final List<String> _memberVariableIds = new ArrayList<>();
  private Set<String> _vocabulary;

  protected VarCollection(Properties properties) {
    _properties = properties;
  }

  public void addMemberVariableId(String memberVariableId) {
    _memberVariableIds.add(memberVariableId);
  }

  /**
   * Performs the following:
   *  1. check num members against actual number of vars discovered
   *  2. checks member vars for having values, matching type/shape against properties of collection
   *  3. tries to populate vocabulary with member var vocabs (result used for unique value count)
   *  4. assign bin widths and units values in subclasses
   *
   * @param entity parent entity of this collection (provides access to variables)
   */
  public void buildAndValidate(Entity entity) {
    if (_properties.numMembers != _memberVariableIds.size()) {
      throw new RuntimeException("Discovered " + _memberVariableIds.size() +
          " variable IDs in collection " + _properties.id + " but " +
          NUM_MEMBERS + " column declares " + _properties.numMembers);
    }
    List<VariableWithValues> valueVars = new ArrayList<>();
    boolean useVocabulary = true;
    Set<String> derivedVocabulary = new HashSet<>();
    for (String varId : _memberVariableIds) {
      Optional<Variable> var = entity.getVariable(varId);
      if (var.isEmpty()) {
        throw new RuntimeException("Collection " + _properties.id +
            " references variable " + varId + " which does not exist in entity " + entity.getId());
      }
      if (!var.get().hasValues() ||
          ((VariableWithValues)var.get()).getType().isSameTypeAs(_properties.type) ||
          ((VariableWithValues)var.get()).getDataShape() == _properties.dataShape) {
        throw new RuntimeException("Variable " + varId + " must have the same " +
            "data type and shape as its parent collection " + _properties.id);
      }
      VariableWithValues valueVar = (VariableWithValues)var.get();
      valueVars.add(valueVar);
      if (valueVar.getVocabulary() != null && !valueVar.getVocabulary().isEmpty()) {
        derivedVocabulary.addAll(valueVar.getVocabulary());
      }
      else {
        // do not declare a vocabular unless all member vars have a vocabulary
        LOG.warn("Member variable " + varId + " of collection " + _properties.id + " does not have a vocabulary.");
        useVocabulary = false;
      }
    }
    // vocabulary will be completely populated or null; hopefully warnings will alert devs of discrepancies
    if (useVocabulary) {
      _vocabulary = derivedVocabulary;
    }
    assignBinValues(valueVars);
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
    return _vocabulary == null ? null : Integer.valueOf(_vocabulary.size()).longValue();
  }

  public List<String> getMemberVariableIds() {
    return _memberVariableIds;
  }

  public Set<String> getVocabulary() {
    return _vocabulary;
  }
}
