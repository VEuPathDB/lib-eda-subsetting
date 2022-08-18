package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultiValueFormatter implements ValueFormatter {

  @Override
  public String format(long idIndex, Iterator<VariableValueIdPair<String>> valuesIterator) {
    VariableValueIdPair<String> pair = valuesIterator.next();
    StringBuilder valueRecord = new StringBuilder("[");
    boolean firstRecord = true;
    while (pair.getIdIndex() == idIndex) {
      if (!firstRecord) {
        valueRecord.append(",");
      } else {
        firstRecord = false;
      }
      valueRecord.append(valuesIterator.next().getValue());
      pair = valuesIterator.next();
    }
    valueRecord.append("]");
    return valueRecord.toString();
  }
}
