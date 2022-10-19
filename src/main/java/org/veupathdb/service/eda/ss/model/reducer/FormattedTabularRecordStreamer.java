package org.veupathdb.service.eda.ss.model.reducer;

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
public class FormattedTabularRecordStreamer implements Iterator<byte[][]> {
  // These value streams need to have associated with them the variable
  private List<ValueStream<String>> valuePairStreams;
  private Iterator<VariableValueIdPair<List<byte[]>>> idMapStream;
  private Iterator<Long> idIndexStream;
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
  public FormattedTabularRecordStreamer(List<ValueStream<String>> valuePairStreams,
                                        Iterator<Long> idIndexStream,
                                        Iterator<VariableValueIdPair<List<byte[]>>> idMapStream) {
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
    VariableValueIdPair<List<byte[]>> ids;
    do {
      ids = idMapStream.next();
    } while (ids.getIdIndex() < currentIdIndex);
    rec = new byte[valuePairStreams.size() + ids.getValue().size()][];
    for (int i = 0; i < ids.getValue().size(); i++) {
      ByteBuffer buf = ByteBuffer.wrap(ids.getValue().get(i));
      int size = buf.getInt();
      rec[recIndex++] = Arrays.copyOfRange(ids.getValue().get(i), Integer.BYTES, size + Integer.BYTES);
    }

    for (ValueStream<String> valueStream: valuePairStreams) {
      if (valueStream.hasNext() && valueStream.peek().getIdIndex() > currentIdIndex) {
        rec[recIndex++] = new byte[0];
        continue;
      }
      // Advance stream until it equals or exceeds the currentIdIndex.
      while (valueStream.hasNext() && valueStream.peek().getIdIndex() < currentIdIndex) {
        valueStream.next();
      }
      if (!valueStream.hasNext() || valueStream.peek().getIdIndex() != currentIdIndex) {
        rec[recIndex++] = new byte[0];
      } else {
        rec[recIndex++] = valueStream.valueFormatter.format(valueStream, currentIdIndex).getBytes(StandardCharsets.UTF_8);
      }
    }
    if (idIndexStream.hasNext()) {
      currentIdIndex = idIndexStream.next();
    } else {
      currentIdIndex = null;
    }
    return rec;
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
