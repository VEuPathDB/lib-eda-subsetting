package org.veupathdb.service.eda.ss.model.reducer;

import org.gusdb.fgputil.collection.InitialSizeStringMap;
import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

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
  private InitialSizeStringMap.Builder outputColBuilder;

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
    this.outputColBuilder = new InitialSizeStringMap.Builder(this.outputColumns);
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
        rec[recIndex++] = valueStream.getValueFormatter().formatString(valueStream, currentIdIndex);
      }
    }
    if (idIndexStream.hasNext()) {
      currentIdIndex = idIndexStream.next();
    } else {
      currentIdIndex = null;
    }
    return outputColBuilder.build().putAll(rec);
  }

  @Override
  public void close() throws Exception {
    idMapStream.close();
    idIndexStream.close();
  }

}
