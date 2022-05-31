package org.veupathdb.service.eda.ss.model.reducer;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.veupathdb.service.eda.ss.model.variable.VariableValue;
import org.veupathdb.service.eda.ss.model.variable.serializer.ValueWithIdSerializer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Predicate;


/**
 * TODO Use ResultConsumer?
 * TODO Maybe take an OptionStream to more easily swap out files
 *
 * Filtered stream
 * @param <V>
 */
public class FilteredValueStream<V> implements AutoCloseable, Iterable<Integer> {
  private Predicate<V> filterPredicate;
  private DualBufferBinaryRecordReader reader;
  private ValueWithIdSerializer<V> serializer;

  public FilteredValueStream(Path path,
                             Predicate<V> filterPredicate,
                             ValueWithIdSerializer<V> serializer) throws IOException {
    this.filterPredicate = filterPredicate;
    this.reader = new DualBufferBinaryRecordReader(path, serializer.totalBytesNeeded(), 1024);
    this.serializer = serializer;
  }

  @Override
  public Iterator<Integer> iterator() {
    return new FilteredIterator();
  }

  @Override
  public void close() {
    reader.close();
  }

  private class FilteredIterator implements Iterator<Integer> {
    private Optional<Integer> next;
    private boolean hasStarted;

    public FilteredIterator() {
      hasStarted = false;
    }

    @Override
    public boolean hasNext() {
      if (!hasStarted) {
        nextMatch();
      }
      return next.isPresent();
    }

    @Override
    public Integer next() {
      if (!hasStarted) {
        nextMatch();
      }
      Integer curr = next.orElseThrow(() -> new NoSuchElementException());
      nextMatch();
      return curr;
    }

    private void nextMatch() {
      do {
        hasStarted = true;
        Optional<byte[]> bytes = reader.next();
        if (bytes.isEmpty()) {
          next = Optional.empty();
          return;
        }
        next = bytes
            .map(serializer::convertFromBytes)
            .filter(var -> filterPredicate.test(var.getValue()))
            .map(VariableValue::getEntityId);
      } while (next.isEmpty());
    }
  }
}
