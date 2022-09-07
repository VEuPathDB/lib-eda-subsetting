package org.veupathdb.service.eda.ss.model.filter;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.StringVariable;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.STRING_VALUE_COL_NAME;

public class StringSetFilter extends SingleValueFilter<byte[], StringVariable> {
  private static final long BIT_MASK = 0xFF;

  private final List<String> _stringSet;
  private final List<byte[]> _byteArrays;
  
  public StringSetFilter(String appDbSchema, Entity entity, StringVariable variable, List<String> stringSet) {
    super(appDbSchema, entity, variable);
    _stringSet = stringSet;
    _byteArrays = stringSet.stream()
        .map(s -> s.getBytes(StandardCharsets.UTF_8))
        .collect(Collectors.toList());
  }

  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + STRING_VALUE_COL_NAME + " IN (" + createSqlInExpression() + ")" + NL;
  }

  @Override
  public Predicate<byte[]> getPredicate() {
    return candidateByteArray -> {
      long stringLength = 0;
      for (int i = 0; i < Integer.BYTES; i++) {
        stringLength <<= 8; // Shift one byte, 8 bits.
        stringLength |= (candidateByteArray[i] & BIT_MASK);
      }
      for (byte[] byteArray: _byteArrays) {
        for (int i = 0; i < stringLength; i++) {
          if (byteArray[i] != candidateByteArray[i + 4]) {
            break;
          }
          if (i == stringLength - 1) {
            return true;
          }
        }
      }
      return false;
    };
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
