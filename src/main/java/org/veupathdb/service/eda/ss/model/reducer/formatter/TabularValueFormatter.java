package org.veupathdb.service.eda.ss.model.reducer.formatter;

import java.util.List;

public interface TabularValueFormatter {

  /**
   * Formats a list of string values to be output in a tabular result. The implementation can differ depending on
   * the type of variable (e.g. multi-value vs. single-value).
   */
  String format(List<String> values);
}
