package org.veupathdb.service.eda.subset.model.filter;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.variable.StringVariable;
import org.veupathdb.service.eda.subset.model.db.DB;

import static org.gusdb.fgputil.FormatUtil.NL;

/**
 * Filter that keeps entities whose (string) variable value starts with any of
 * the supplied prefixes, i.e. the union (OR) of the individual prefix matches.
 *
 * The motivating use case is filtering by geohashes of mixed precision: a
 * client can describe an arbitrary geographic shape as a compact set of
 * geohash prefixes of different lengths (coarse hashes covering the interior,
 * finer hashes along the boundary) and apply them to the entity's
 * finest-grained geohash variable with a single filter, since a geohash
 * prefix-matches all finer geohashes it contains.
 */
public class StringPrefixSetFilter extends SingleValueFilter<byte[], StringVariable> {
  private static final long BIT_MASK = 0xFF;

  private final List<String> _prefixSet;
  private final List<byte[]> _prefixSetByteArrays;

  public StringPrefixSetFilter(String appDbSchema, Entity entity, StringVariable variable, List<String> prefixSet) {
    super(appDbSchema, entity, variable);
    if (prefixSet.isEmpty())
      throw new IllegalArgumentException("String prefix set filter: >0 prefixes must be specified");
    if (prefixSet.stream().anyMatch(String::isEmpty))
      throw new IllegalArgumentException("String prefix set filter: prefixes must not be empty strings");
    _prefixSet = prefixSet;
    _prefixSetByteArrays = prefixSet.stream()
      .map(s -> s.getBytes(StandardCharsets.UTF_8))
      .collect(Collectors.toList());
  }

  // safe from SQL injection: values are escaped in createLikeClauses()
  @Override
  public String getFilteringAndClausesSql() {
    return "  AND (" + createLikeClauses() + ")" + NL;
  }

  @Override
  public Predicate<byte[]> getPredicate() {
    return candidateByteArray -> {
      long candidateLength = 0;
      for (int i = 0; i < Integer.BYTES; i++) {
        candidateLength <<= 8; // Shift one byte, 8 bits.
        candidateLength |= (candidateByteArray[i] & BIT_MASK);
      }
      for (byte[] prefix : _prefixSetByteArrays) {
        if (prefix.length > candidateLength) {
          // Shortcut skip if the prefix is longer than the candidate.
          continue;
        }
        for (int i = 0; i < prefix.length; i++) {
          // If any character is mismatched, skip this prefix.
          if (prefix[i] != candidateByteArray[i + Integer.BYTES]) {
            break;
          }
          // Made it to the end of the prefix, this is a match.
          if (i == prefix.length - 1) {
            return true;
          }
        }
      }
      return false;
    };
  }

  private String createLikeClauses() {
    return _prefixSet.stream()
      // escape the LIKE wildcard characters, using backslash as the escape character
      .map(s -> s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_"))
      // replace single quotes with two single quotes (sql escape)
      .map(s -> s.replaceAll("'", "''"))
      // build a prefix LIKE clause
      .map(s -> DB.Tables.AttributeValue.Columns.STRING_VALUE_COL_NAME + " LIKE '" + s + "%' ESCAPE '\\'")
      // join with OR: an entity matches if any prefix matches
      .collect(Collectors.joining(" OR "));
  }

}
