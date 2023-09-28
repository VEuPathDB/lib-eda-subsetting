package org.veupathdb.service.eda.ss.model.reducer;

import org.gusdb.fgputil.collection.InitialSizeStringMap;
import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.reducer.formatter.TabularValueFormatter;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Provides a stream of variable value records extracted from a stream of ID indexes and a collection of
 * ID index, variable value pairs. The resulting stream provides formatted tuples of strings containing entity IDs,
 * ancestor IDs and all associated variable values.
 */
public class UnformattedTabularRecordStreamer implements CloseableIterator<Map<String, String>> {
  // These value streams need to have associated with them the variable
  private List<ValueStream<String>> valuePairStreams;
  private CloseableIterator<VariableValueIdPair<List<String>>> idMapStream;
  private CloseableIterator<Long> idIndexStream;
  private Long currentIdIndex;
  private String[] outputColumns;

  /**
   * Constructs an instance, which provides a stream of string-formatted records. The stream is composed of all IDs
   * in the idIndex stream, corresponding ancestor IDs corresponding values in the
   * {@link UnformattedTabularRecordStreamer#valuePairStreams}
   *
   * @param valuePairStreams Streams of pairs containing entity ID indexes and corresponding values for their respective
   *                         variables.
   * @param idIndexStream    Stream of ID indexes, indicating which entity records to output.
   * @param idMapStream
   */
  public UnformattedTabularRecordStreamer(List<ValueStream<String>> valuePairStreams,
                                          CloseableIterator<Long> idIndexStream,
                                          CloseableIterator<VariableValueIdPair<List<String>>> idMapStream,
                                          List<String> outputColumns) {
    this.valuePairStreams = valuePairStreams;
    this.idIndexStream = idIndexStream;
    if (idIndexStream.hasNext()) {
      currentIdIndex = idIndexStream.next();
    }
    this.idMapStream = idMapStream;
    this.outputColumns = outputColumns.toArray(String[]::new);
  }

  @Override
  public boolean hasNext() {
    return currentIdIndex != null;
  }

  @Override
  public Map<String, String> next() {
    if (currentIdIndex == null) {
      throw new NoSuchElementException("No tabular records remain in stream.");
    }
    VariableValueIdPair<List<String>> ids;
    int recIndex = 0;

    do {
      ids = idMapStream.next();
    } while (ids.getIdIndex() < currentIdIndex);
    String[] rec = new String[valuePairStreams.size() + ids.getValue().size()];
    for (int i = 0; i < ids.getValue().size(); i++) {
      rec[recIndex++] = ids.getValue().get(i);
    }

    for (ValueStream<String> valueStream: valuePairStreams) {
      if (valueStream.hasNext() && valueStream.peek().getIdIndex() > currentIdIndex) {
        rec[recIndex++] = "";
        continue;
      }
      // Advance stream until it equals or exceeds the currentIdIndex.
      while (valueStream.hasNext() && valueStream.peek().getIdIndex() < currentIdIndex) {
        valueStream.next();
      }
      if (!valueStream.hasNext() || !Objects.equals(valueStream.peek().getIdIndex(), currentIdIndex)) {
        rec[recIndex++] = "";
      } else {
        rec[recIndex++] = valueStream.valueFormatter.formatString(valueStream, currentIdIndex);
      }
    }
    if (idIndexStream.hasNext()) {
      currentIdIndex = idIndexStream.next();
    } else {
      currentIdIndex = null;
    }
    return new InitialSizeStringMap.Builder(outputColumns).build().putAll(rec);
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
