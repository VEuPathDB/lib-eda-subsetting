package org.veupathdb.service.eda.ss.model.reducer;

import org.gusdb.fgputil.iterator.CloseableIterator;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class CountingCloseableIterator<T> implements CloseableIterator<T> {
  private final CloseableIterator<T> iteratorToCount;
  private final AtomicLong count = new AtomicLong(0L);

  public CountingCloseableIterator(CloseableIterator<T> iteratorToCount) {
    this.iteratorToCount = iteratorToCount;
  }

  @Override
  public boolean hasNext() {
    return iteratorToCount.hasNext();
  }

  @Override
  public T next() {
    count.incrementAndGet();
    return iteratorToCount.next();
  }

  public long count() {
    return count.get();
  }

  @Override
  public void close() throws Exception {
    iteratorToCount.close();
  }
}
