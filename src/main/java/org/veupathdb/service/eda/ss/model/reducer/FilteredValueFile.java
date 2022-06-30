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


/***
 * Stream of data produced by reading a binary-encoded file with entity ID indexes and variable values.
 * Outputs the ID indexes while applying a filter to the variable values.
 * @param <V> type of the value
 */
public class FilteredValueFile<V, T> implements AutoCloseable, Iterator<T> {
  private final Predicate<V> filterPredicate;
  private final DualBufferBinaryRecordReader reader;
  private final BinaryDeserializer<VariableValueIdPair<V>> deserializer;
  private final Function<VariableValueIdPair<V>, T> pairExtractor;
  private T next;
  private boolean hasStarted;

  public FilteredValueFile(Path path,
                           Predicate<V> filterPredicate,
                           BinaryDeserializer<VariableValueIdPair<V>> deserializer,
                           Function<VariableValueIdPair<V>, T> pairExtractor) throws IOException {
    final byte[] byteBuffer = new byte[deserializer.numBytes()];
    this.filterPredicate = filterPredicate;
    this.reader = new DualBufferBinaryRecordReader(path,
        deserializer.numBytes(),
        1024,
        () -> byteBuffer);
    this.deserializer = deserializer;
    this.pairExtractor = pairExtractor;
  }

  @Override
  public boolean hasNext() {
    if (!hasStarted) {
      nextMatch();
    }
    return next != null;
  }

  @Override
  public T next() {
    if (!hasStarted) {
      nextMatch();
    }
    T curr = next;
    if (curr == null) {
      throw new NoSuchElementException();
    }
    nextMatch();
    return curr;
  }

  private void nextMatch() {
    hasStarted = true;
    do {
      Optional<byte[]> bytes = reader.next();
      if (bytes.isEmpty()) {
        next = null;
        return;
      }
      VariableValueIdPair<V> valuePair = deserializer.fromBytes(bytes.get());
      if (filterPredicate.test(valuePair.getValue())) {
        next = pairExtractor.apply(valuePair);
      } else {
        next = null;
      }
    } while (next == null);
  }

  @Override
  public void close() {
    reader.close();
  }
}
