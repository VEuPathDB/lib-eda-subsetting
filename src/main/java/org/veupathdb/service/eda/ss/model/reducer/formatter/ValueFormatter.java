package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.Iterator;
import java.util.List;

public interface ValueFormatter {

  String format(long idIndex, Iterator<VariableValueIdPair<String>> valuesIterator);
}
