package org.veupathdb.service.eda.ss.model.reducer.formatter;

import java.util.List;
import java.util.stream.Collectors;

public class MultiValueFormatter implements TabularValueFormatter {

  @Override
  public String format(List<String> values) {
    StringBuilder valueRecord = new StringBuilder("[");
    valueRecord.append(values.stream().collect(Collectors.joining(",")));
    valueRecord.append("]");
    return valueRecord.toString();
  }
}
