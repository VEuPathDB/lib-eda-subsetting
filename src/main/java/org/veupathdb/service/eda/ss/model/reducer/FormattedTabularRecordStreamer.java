package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.*;
import java.util.stream.Collectors;

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
  private ValueStream<List<Long>> ancestorStream;
  private Iterator<Long> idIndexStream;
  private Long currentIdIndex;
  private Entity outputEntity;

  /**
   * Constructs an instance, which provides a stream of string-formatted records. The stream is composed of all IDs
   * in the idIndex stream, corresponding ancestor IDs corresponding values in the
   * {@link FormattedTabularRecordStreamer#valuePairStreams}
   *
   * @param valuePairStreams Streams of pairs containing entity ID indexes and corresponding values for their respective
   *                         variables.
   * @param idIndexStream    Stream of ID indexes, indicating which entity records to output.
   * @param ancestorStream   Stream of ancestors, used to merge in ancestors of target output entities in output records.
   * @param outputEntity     Entity type of records which will be output.
   */
  public FormattedTabularRecordStreamer(List<Iterator<VariableValueIdPair<String>>> valuePairStreams,
                                        Iterator<Long> idIndexStream,
                                        Iterator<VariableValueIdPair<List<Long>>> ancestorStream,
                                        Entity outputEntity) {
    this.valuePairStreams = valuePairStreams.stream()
        .map(s -> new ValueStream<>(s))
        .collect(Collectors.toList());
    this.idIndexStream = idIndexStream;
    if (idIndexStream.hasNext()) {
      currentIdIndex = idIndexStream.next();
    }
    if (ancestorStream != null) {
      this.ancestorStream = new ValueStream<>(ancestorStream);
    }
    this.outputEntity = outputEntity;
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
    record.add(currentIdIndex.toString());
    // Only add ancestors if entity has ancestors associated with it.
    // TODO: Don't run this for every row if we don't have ancestors.
    if (!outputEntity.getAncestorEntities().isEmpty()) {
      // <= and next()
      while (ancestorStream.peek().getIdIndex() < currentIdIndex) {
        ancestorStream.next();
      }
      // Add all ancestor IDs
      ancestorStream.next().getValue().stream()
          .map(Objects::toString)
          .forEach(record::add);
    }
    for (ValueStream<String> valueStream: valuePairStreams) {
      // Advance stream until it equals or exceeds the currentIdIndex
      while (valueStream.hasNext() && valueStream.peek().getIdIndex() < currentIdIndex) {
        valueStream.next();
      }
      if (!valueStream.hasNext()) {
        record.add("");
        continue;
      }
      if (valueStream.peek().getIdIndex() == currentIdIndex) {
        record.add(valueStream.next().getValue());
      } else {
        record.add("");
      }
    }
    if (idIndexStream.hasNext()) {
      currentIdIndex = idIndexStream.next();
    } else {
      currentIdIndex = null;
    }
    return record;
  }

  private static class ValueStream<T> implements Iterator<VariableValueIdPair<T>> {
    private VariableValueIdPair<T> next;
    private final Iterator<VariableValueIdPair<T>> stream;

    public ValueStream(Iterator<VariableValueIdPair<T>> stream) {
      if (stream.hasNext()) {
        next = stream.next();
      }
      this.stream = stream;
    }

    public VariableValueIdPair<?> peek() {
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
