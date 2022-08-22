package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.reducer.formatter.ValueFormatter;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.*;

/**
 * Provides a stream of variable value records extracted from a stream of ID indexes and a collection of
 * ID index, variable value pairs. The resulting stream provides formatted tuples of strings containing entity IDs,
 * ancestor IDs and all associated variable values.
 *
 * TODO Handle header lines.
 */
public class FormattedTabularRecordStreamer implements Iterator<List<String>> {
  // These value streams need to have associated with them the variable
  private List<ValueStream<String>> valuePairStreams;
  private Iterator<VariableValueIdPair<List<String>>> idMapStream;
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
                                        Iterator<VariableValueIdPair<List<String>>> idMapStream) {
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
  public List<String> next() {
    if (currentIdIndex == null) {
      throw new NoSuchElementException("No tabular records remain in stream.");
    }
    List<String> record = new ArrayList<>();

    VariableValueIdPair<List<String>> ids;
    do {
      ids = idMapStream.next();
    } while (ids.getIdIndex() < currentIdIndex);
    record.addAll(ids.getValue());

    for (ValueStream<String> valueStream: valuePairStreams) {
      // Advance stream until it equals or exceeds the currentIdIndex.
      while (valueStream.hasNext() && valueStream.peek().getIdIndex() < currentIdIndex) {
        valueStream.next();
      }
      if (!valueStream.hasNext()) {
        record.add("");
      } else {
        // Accumulate values with the given ID index to pass to the formatter.
        List<String> values = new ArrayList<>();
        while (valueStream.hasNext() && valueStream.peek().getIdIndex() == currentIdIndex) {
          values.add(valueStream.next().getValue());
        }
        record.add(valueStream.valueFormatter.format(values));
      }
    }
    if (idIndexStream.hasNext()) {
      currentIdIndex = idIndexStream.next();
    } else {
      currentIdIndex = null;
    }
    return record;
  }

  public static class ValueStream<T> implements Iterator<VariableValueIdPair<T>> {
    private VariableValueIdPair<T> next;
    private final Iterator<VariableValueIdPair<T>> stream;
    private final ValueFormatter valueFormatter;

    public ValueStream(Iterator<VariableValueIdPair<T>> stream, ValueFormatter valueFormatter) {
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
