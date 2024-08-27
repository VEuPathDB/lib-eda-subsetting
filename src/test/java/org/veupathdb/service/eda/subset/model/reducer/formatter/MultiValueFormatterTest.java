package org.veupathdb.service.eda.subset.model.reducer.formatter;

import org.gusdb.fgputil.iterator.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.subset.model.reducer.ValueStream;
import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class MultiValueFormatterTest {

  @Test
  public void testMultiFormatter() {
    List<VariableValueIdPair<byte[]>> utf8Strings = List.of(
        new VariableValueIdPair<>(0, "0-first".getBytes(StandardCharsets.UTF_8)),
        new VariableValueIdPair<>(0, "0-second".getBytes(StandardCharsets.UTF_8)),
        new VariableValueIdPair<>(1, "1".getBytes(StandardCharsets.UTF_8)),
        new VariableValueIdPair<>(3, "3".getBytes(StandardCharsets.UTF_8))
    );
    MultiValueFormatter formatter = new MultiValueFormatter();
    ValueStream<byte[]> valueStream = new ValueStream<>(CloseableIterator.of(utf8Strings.iterator()), formatter);
    Assertions.assertEquals("[0-first,0-second]", new String(formatter.format(valueStream, 0), StandardCharsets.UTF_8));
    Assertions.assertEquals("[1]", new String(formatter.format(valueStream, 1), StandardCharsets.UTF_8));
    Assertions.assertEquals("[3]", new String(formatter.format(valueStream, 3), StandardCharsets.UTF_8));
  }
}