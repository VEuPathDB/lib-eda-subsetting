package org.veupathdb.service.eda.ss.model.reducer.formatter;

import java.util.List;

public class SingleValueFormatter implements TabularValueFormatter {

  @Override
  public String format(List<String> values) {
    return values.get(0);
  }
}
