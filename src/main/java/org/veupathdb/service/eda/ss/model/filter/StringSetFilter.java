package org.veupathdb.service.eda.ss.model.filter;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.StringVariable;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.STRING_VALUE_COL_NAME;

public class StringSetFilter extends SingleValueFilter<String, StringVariable> {

  private final List<String> _stringSet;
  
  public StringSetFilter(String appDbSchema, Entity entity, StringVariable variable, List<String> stringSet) {
    super(appDbSchema, entity, variable);
    _stringSet = stringSet;
  }

  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + STRING_VALUE_COL_NAME + " IN (" + createSqlInExpression() + ")" + NL;
  }

  @Override
  public Predicate<String> getPredicate() {
    return strVar -> _stringSet.contains(strVar);
  }

  private String createSqlInExpression() {

    // validate values against this var's vocabulary
    /* FIXME: add back at a less risky time, after validating DB data (ClinEpi Phase 2?)
    List<String> vocab = Optional.ofNullable(_variable.getVocabulary())
        .orElseThrow(() -> new RuntimeException("Var " + _variable.getId() + " has null vocabulary."));
    for (String value : _stringSet) {
      if (!vocab.contains(value)) {
        throw new BadRequestException("Value '" + value +
            "' is not in the vocabulary of variable '" +
            _variable.getId() + "' [ " + String.join(", ", vocab) + " ].");
      }
    }*/

    // process the validated list
    return _stringSet.stream()
        // replace single quotes with two single quotes (sql escape)
        .map(s -> s.replaceAll("'", "''"))
        // wrap in quotes
        .map(s -> "'" + s + "'")
        // join with commas
        .collect(Collectors.joining(", "));
  }

}
