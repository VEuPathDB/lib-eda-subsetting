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
  private final List<byte[]> _stringSetByteArrays;
  
  public StringSetFilter(String appDbSchema, Entity entity, StringVariable variable, List<String> stringSet) {
    super(appDbSchema, entity, variable);
    _stringSet = stringSet;
    _stringSetByteArrays = stringSet.stream()
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
      long candidateLength = 0;
      for (int i = 0; i < Integer.BYTES; i++) {
        candidateLength <<= 8; // Shift one byte, 8 bits.
        candidateLength |= (candidateByteArray[i] & BIT_MASK);
      }
      for (byte[] byteArray : _stringSetByteArrays) {
        if (candidateLength != byteArray.length) {
          // Shortcut skip if length is not equal.
          continue;
        }
        for (int i = 0; i < candidateLength; i++) {
          // If any character is mismatched, skip this record.
          if (byteArray[i] != candidateByteArray[i + Integer.BYTES]) {
            break;
          }
          // Made it to the end of the string, this is a match.
          if (i == candidateLength - 1) {
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
