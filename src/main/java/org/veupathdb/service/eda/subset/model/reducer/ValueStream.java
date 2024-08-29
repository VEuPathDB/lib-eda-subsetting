package org.veupathdb.service.eda.subset.model.reducer;

import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.subset.model.reducer.formatter.TabularValueFormatter;
import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;

import java.util.Iterator;

public class ValueStream<T> implements Iterator<VariableValueIdPair<T>> {
  private VariableValueIdPair<T> next;
  private final Iterator<VariableValueIdPair<T>> stream;
  private final TabularValueFormatter valueFormatter;

  public ValueStream(CloseableIterator<VariableValueIdPair<T>> stream, TabularValueFormatter valueFormatter) {
    this.stream = stream;
    this.valueFormatter = valueFormatter;
    if (stream.hasNext()) {
      next = stream.next();
    }
  }

  public VariableValueIdPair<T> peek() {
    return next;
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public VariableValueIdPair<T> next() {
    VariableValueIdPair<T> curr = next;
    if (stream.hasNext()) {
      next = stream.next();
    } else {
      next = null;
    }
    return curr;
  }

  public TabularValueFormatter getValueFormatter() {
    return valueFormatter;
  }
}
