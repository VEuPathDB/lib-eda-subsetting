package org.veupathdb.service.eda.ss.model.reducer;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryDeserializer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;


/**
 * TODO Use ResultConsumer?
 * TODO Maybe take an OptionStream to more easily swap out files
 *
 * Filtered stream
 * @param <V> type of the value
 */
public class FilteredValueFile<V, T> implements AutoCloseable, Iterator<T> {
  private final Predicate<V> filterPredicate;
  private final DualBufferBinaryRecordReader reader;
  private final BinaryDeserializer<VariableValueIdPair<V>> serializer;
  private final Function<VariableValueIdPair<V>, T> pairExtractor;
  private Optional<T> next;
  private boolean hasStarted;

  public FilteredValueFile(Path path,
                           Predicate<V> filterPredicate,
                           BinaryDeserializer<VariableValueIdPair<V>> deserializer,
                           Function<VariableValueIdPair<V>, T> pairExtractor) throws IOException {
    this.filterPredicate = filterPredicate;
    this.reader = new DualBufferBinaryRecordReader(path, deserializer.numBytes(), 1024);
    this.serializer = deserializer;
    this.pairExtractor = pairExtractor;
  }

  @Override
  public boolean hasNext() {
    if (!hasStarted) {
      nextMatch();
    }
    return next.isPresent();
  }

  @Override
  public T next() {
    if (!hasStarted) {
      nextMatch();
    }
    T curr = next.orElseThrow(NoSuchElementException::new);
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
          .map(var -> pairExtractor.apply(var));
    } while (next.isEmpty());
  }

  @Override
  public void close() {
    reader.close();
  }
}
