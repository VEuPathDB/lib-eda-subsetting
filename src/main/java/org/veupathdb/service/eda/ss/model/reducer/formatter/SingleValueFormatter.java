package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.Iterator;

public class SingleValueFormatter implements ValueFormatter {

  @Override
  public String format(long idIndex, Iterator<VariableValueIdPair<String>> valuesIterator) {
    VariableValueIdPair<String> pair = valuesIterator.next();
    if (pair.getIdIndex() == idIndex) {
      return pair.getValue();
    } else {
      return "";
    }
  }
}
