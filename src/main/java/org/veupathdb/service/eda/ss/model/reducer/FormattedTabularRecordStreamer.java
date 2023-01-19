package org.veupathdb.service.eda.ss.model.reducer;

import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.reducer.formatter.TabularValueFormatter;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Provides a stream of variable value records extracted from a stream of ID indexes and a collection of
 * ID index, variable value pairs. The resulting stream provides formatted tuples of strings containing entity IDs,
 * ancestor IDs and all associated variable values.
 */
public class FormattedTabularRecordStreamer implements CloseableIterator<byte[][]> {
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  // These value streams need to have associated with them the variable
  private List<ValueStream<byte[]>> valuePairStreams;
  private CloseableIterator<VariableValueIdPair<byte[][]>> idMapStream;
  private CloseableIterator<Long> idIndexStream;
  private Long currentIdIndex;

  /**
   * Constructs an instance, which provides a stream of string-formatted records. The stream is composed of all IDs
   * in the idIndex stream, corresponding ancestor IDs corresponding values in the
   * {@link FormattedTabularRecordStreamer#valuePairStreams}
   *
   * @param valuePairStreams Streams of pairs containing entity ID indexes and corresponding values for their respective
   *                         variables.
   * @param idIndexStream    Stream of ID indexes, indicating which entity records to output.
   * @param idMapStream
   */
  public FormattedTabularRecordStreamer(List<ValueStream<byte[]>> valuePairStreams,
                                        CloseableIterator<Long> idIndexStream,
                                        CloseableIterator<VariableValueIdPair<byte[][]>> idMapStream) {
    this.valuePairStreams = valuePairStreams;
    this.idIndexStream = idIndexStream;
    if (idIndexStream.hasNext()) {
      currentIdIndex = idIndexStream.next();
    }
    this.idMapStream = idMapStream;
  }

  @Override
  public boolean hasNext() {
    return currentIdIndex != null;
  }

  @Override
  public byte[][] next() {
    if (currentIdIndex == null) {
      throw new NoSuchElementException("No tabular records remain in stream.");
    }
    byte[][] rec;
    int recIndex = 0;
    VariableValueIdPair<byte[][]> ids;
    do {
      ids = idMapStream.next();
    } while (ids.getIdIndex() < currentIdIndex);
    rec = new byte[valuePairStreams.size() + ids.getValue().length][];
    for (int i = 0; i < ids.getValue().length; i++) {
      ByteBuffer buf = ByteBuffer.wrap(ids.getValue()[i]);
      int size = buf.getInt();
      rec[recIndex++] = Arrays.copyOfRange(ids.getValue()[i], Integer.BYTES, size + Integer.BYTES);
    }

    for (ValueStream<byte[]> valueStream: valuePairStreams) {
      if (valueStream.hasNext() && valueStream.peek().getIdIndex() > currentIdIndex) {
        rec[recIndex++] = EMPTY_BYTE_ARRAY;
        continue;
      }
      // Advance stream until it equals or exceeds the currentIdIndex.
      while (valueStream.hasNext() && valueStream.peek().getIdIndex() < currentIdIndex) {
        valueStream.next();
      }
      if (!valueStream.hasNext() || !Objects.equals(valueStream.peek().getIdIndex(), currentIdIndex)) {
        rec[recIndex++] = EMPTY_BYTE_ARRAY;
      } else {
        rec[recIndex++] = valueStream.valueFormatter.format(valueStream, currentIdIndex);
      }
    }
    if (idIndexStream.hasNext()) {
      currentIdIndex = idIndexStream.next();
    } else {
      currentIdIndex = null;
    }
    return rec;
  }

  @Override
  public void close() throws Exception {
    idMapStream.close();
    idIndexStream.close();
  }

  public static class ValueStream<T> implements Iterator<VariableValueIdPair<T>> {
    private VariableValueIdPair<T> next;
    private final Iterator<VariableValueIdPair<T>> stream;
    private final TabularValueFormatter valueFormatter;

    public ValueStream(Iterator<VariableValueIdPair<T>> stream, TabularValueFormatter valueFormatter) {
      this.stream = stream;
      this.valueFormatter = valueFormatter;
      if (stream.hasNext()) {
        next = stream.next();
      }
    }

    public VariableValueIdPair<T> peek() {
      return next;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public VariableValueIdPair<T> next() {
      VariableValueIdPair<T> curr = next;
      if (stream.hasNext()) {
        next = stream.next();
      } else {
        next = null;
      }
      return curr;
    }
  }
}
