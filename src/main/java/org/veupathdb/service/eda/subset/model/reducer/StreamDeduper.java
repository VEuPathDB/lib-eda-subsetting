package org.veupathdb.service.eda.subset.model.reducer;

import org.gusdb.fgputil.iterator.CloseableIterator;

public class StreamDeduper implements CloseableIterator<Long> {
  private final CloseableIterator<Long> stream;
  private Long previous;

  public StreamDeduper(CloseableIterator<Long> stream) {
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

  @Override
  public void close() throws Exception {
    stream.close();
  }
}
