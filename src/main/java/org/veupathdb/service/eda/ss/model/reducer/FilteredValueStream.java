package org.veupathdb.service.eda.ss.model.reducer;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueWithIdSerializer;

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
 * @param <V> type of the value
 */
public class FilteredValueStream<V> implements AutoCloseable, Iterator<String> {
  private final Predicate<V> filterPredicate;
  private final DualBufferBinaryRecordReader reader;
  private final ValueWithIdSerializer<V> serializer;
  private Optional<String> next;
  private boolean hasStarted;

  public FilteredValueStream(Path path,
                             Predicate<V> filterPredicate,
                             ValueWithIdSerializer<V> serializer) throws IOException {
    this.filterPredicate = filterPredicate;
    this.reader = new DualBufferBinaryRecordReader(path, serializer.totalBytesNeeded(), 1024);
    this.serializer = serializer;
  }

  @Override
  public boolean hasNext() {
    if (!hasStarted) {
      nextMatch();
    }
    return next.isPresent();
  }

  @Override
  public String next() {
    if (!hasStarted) {
      nextMatch();
    }
    String curr = next.orElseThrow(() -> new NoSuchElementException());
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
          .map(VariableValueIdPair::getEntityId);
    } while (next.isEmpty());
  }

  @Override
  public void close() throws Exception {
    reader.close();
  }
}
