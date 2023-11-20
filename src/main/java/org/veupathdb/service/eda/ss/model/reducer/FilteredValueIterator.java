package org.veupathdb.service.eda.ss.model.reducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.DualBufferBinaryRecordReader;
import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryDeserializer;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;


/***
 * Stream of data produced by reading a binary-encoded file with entity ID indexes and variable values.
 * Outputs the ID indexes while applying a filter to the variable values.
 * @param <V> type of the value returned by the iterator
 */
public class FilteredValueIterator<V, T> implements CloseableIterator<T> {
  private static final Logger LOG = LogManager.getLogger(FilteredValueIterator.class);

  private final Predicate<V> filterPredicate;
  private final DualBufferBinaryRecordReader<VariableValueIdPair<V>> reader;
  private final Function<VariableValueIdPair<V>, T> idValuePairMapper;
  private T next;
  private boolean hasStarted;

  /**
   * Constructs an instance of a FilteredValueIterator.
   *
   * @param path Path to file containing binary-encoded variable ID index, variable value tuples.
   * @param filterPredicate Predicate applied to variable values to filter tuples.
   * @param deserializer Deserializer used to deserialize binary-encoded data.
   * @param idValuePairMapper Function applied to determine data to extract from tuples and output in the stream. In some
   *                      instances, we may only need the ID indexes while in others, we might want to extract the
   *                      values or the entire tuple.
   * @throws IOException If an I/O error occurs while opening binary file.
   */
  public FilteredValueIterator(Path path,
                               Predicate<V> filterPredicate,
                               BinaryDeserializer<? extends VariableValueIdPair<V>> deserializer,
                               Function<VariableValueIdPair<V>, T> idValuePairMapper,
                               ExecutorService fileChannelThreadPool,
                               ExecutorService deserializerThreadPool) throws IOException {
    this.filterPredicate = filterPredicate;
    this.reader = new DualBufferBinaryRecordReader<>(path,
        deserializer.numBytes(),
        1024,
        deserializer::fromBytes,
        fileChannelThreadPool,
        deserializerThreadPool
    );
    this.idValuePairMapper = idValuePairMapper;
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
      if (!reader.hasNext()) {
        next = null;
        return;
      }
      VariableValueIdPair<V> valuePair = reader.next();
      if (filterPredicate.test(valuePair.getValue())) {
        next = idValuePairMapper.apply(valuePair);
      } else {
        next = null;
      }
    } while (next == null);
  }

  @Override
  public void close() {
    LOG.info("Total time spent awaiting disk reads: {}", Duration.ofMillis(reader.getTimeAwaitingFill()));
    LOG.info("*************TESTING************************");
    reader.close();
  }
}
