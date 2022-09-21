package org.veupathdb.service.eda.ss.model.reducer;

import java.util.Iterator;
import java.util.List;

public class StreamDeduper implements Iterator<Long> {
  private Iterator<Long> stream;
  private Long previous;

  public StreamDeduper(Iterator<Long> stream) {
    this.stream = stream;
    this.previous = null;
  }

  @Override
  public boolean hasNext() {
    return stream.hasNext();
  }

  @Override
  public Long next() {
    if (previous == null) {
      previous = stream.next();
      return previous;
    }
    Long curr = stream.next();
    while (previous.equals(curr)) {
      curr = stream.next();
    }
    previous = curr;
    return curr;
  }
}
