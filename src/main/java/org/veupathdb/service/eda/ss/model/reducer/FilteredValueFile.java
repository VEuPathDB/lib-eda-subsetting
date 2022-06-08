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
public class FilteredValueFile<V> implements AutoCloseable, Iterator<Long> {
  private final Predicate<V> filterPredicate;
  private final DualBufferBinaryRecordReader reader;
  private final ValueWithIdSerializer<V> serializer;
  private Optional<Long> next;
  private boolean hasStarted;

  public FilteredValueFile(Path path,
                           Predicate<V> filterPredicate,
                           ValueWithIdSerializer<V> serializer) throws IOException {
    this.filterPredicate = filterPredicate;
    this.reader = new DualBufferBinaryRecordReader(path, serializer.numBytes(), 1024);
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
  public Long next() {
    if (!hasStarted) {
      nextMatch();
    }
    Long curr = next.orElseThrow(NoSuchElementException::new);
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
          .map(serializer::fromBytes)
          .filter(var -> filterPredicate.test(var.getValue()))
          .map(VariableValueIdPair::getIndex);
    } while (next.isEmpty());
  }

  @Override
  public void close() {
    reader.close();
  }
}