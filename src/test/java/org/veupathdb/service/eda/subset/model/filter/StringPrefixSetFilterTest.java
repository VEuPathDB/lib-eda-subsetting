package org.veupathdb.service.eda.subset.model.filter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Predicate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.variable.StringVariable;
import org.veupathdb.service.eda.subset.model.variable.VariableType;
import org.veupathdb.service.eda.subset.testutil.TestDataProvider;

public class StringPrefixSetFilterTest {

  @Test
  public void testGeohashPrefixesOfMixedLength() {
    StringPrefixSetFilter filter = constructFilter(List.of("dr5", "dr72", "gcpvj"));
    Predicate<byte[]> predicate = filter.getPredicate();

    // matched by the coarse prefix "dr5"
    Assertions.assertTrue(predicate.test(encode("dr5")));
    Assertions.assertTrue(predicate.test(encode("dr5ru7")));
    // matched by "dr72"
    Assertions.assertTrue(predicate.test(encode("dr72h8")));
    // matched by "gcpvj"
    Assertions.assertTrue(predicate.test(encode("gcpvj0")));

    // near misses
    Assertions.assertFalse(predicate.test(encode("dr7")));    // shorter than prefix "dr72"
    Assertions.assertFalse(predicate.test(encode("dr6ru7"))); // mismatched third character
    Assertions.assertFalse(predicate.test(encode("gcpu")));   // mismatched fourth character
    Assertions.assertFalse(predicate.test(encode("xdr5")));   // prefix must anchor at the start
  }

  @Test
  public void testSqlEscapesLikeWildcardsAndQuotes() {
    StringPrefixSetFilter filter = constructFilter(List.of("a%b_c'd\\e"));
    String sql = filter.getFilteringAndClausesSql();
    Assertions.assertTrue(sql.contains("LIKE 'a\\%b\\_c''d\\\\e%' ESCAPE '\\'"),
        "unexpected SQL: " + sql);
  }

  @Test
  public void testRejectsEmptyPrefixList() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> constructFilter(List.of()));
  }

  @Test
  public void testRejectsEmptyPrefix() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> constructFilter(List.of("dr5", "")));
  }

  private StringPrefixSetFilter constructFilter(List<String> prefixes) {
    final Entity entity = TestDataProvider.constructEntity();
    final StringVariable stringVariable = new TestDataProvider.StringVariableBuilder()
      .withEntity(entity)
      .withVariableId("var-geohash-6")
      .withVariableType(VariableType.STRING)
      .build();
    entity.addVariable(stringVariable);
    return new StringPrefixSetFilter("unit-test", entity, stringVariable, prefixes);
  }

  /**
   * Encode a string value the way values appear in the binary files read by
   * {@link StringPrefixSetFilter#getPredicate()}: a 4-byte big-endian length
   * followed by the UTF-8 bytes.
   */
  private static byte[] encode(String value) {
    byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    return ByteBuffer.allocate(Integer.BYTES + utf8.length)
      .putInt(utf8.length)
      .put(utf8)
      .array();
  }
}
