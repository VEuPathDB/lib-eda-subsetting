package org.veupathdb.service.eda.ss.model.reducer;

import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryDeserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
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
  private final DualBufferBinaryRecordReader<VariableValueIdPair<V>> reader;
  private final BinaryDeserializer<VariableValueIdPair<V>> deserializer;
  private final Function<VariableValueIdPair<V>, T> pairExtractor;
  private Optional<T> next;
  private boolean hasStarted;

  public FilteredValueFile(Path path,
                           Predicate<V> filterPredicate,
                           BinaryDeserializer<VariableValueIdPair<V>> deserializer,
                           Function<VariableValueIdPair<V>, T> pairExtractor) throws IOException {
    this.filterPredicate = filterPredicate;
    this.reader = new DualBufferBinaryRecordReader<>(path,
        deserializer.numBytes(),
        1024,
        deserializer::fromBytes,
        ByteBuffer.allocate(deserializer.numBytes()));
    this.deserializer = deserializer;
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
    hasStarted = true;
    do {
      Optional<VariableValueIdPair<V>> value = reader.next();
      if (value.isEmpty()) {
        next = Optional.empty();
        return;
      }
      if (filterPredicate.test(value.get().getValue())) {
        next = Optional.of(pairExtractor.apply(value.get()));
      } else {
        next = Optional.empty();
      }
    } while (next.isEmpty());
  }

  @Override
  public void close() {
    reader.close();
  }
}
