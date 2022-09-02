package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.reducer.FormattedTabularRecordStreamer;

import java.util.List;
import java.util.stream.Collectors;

public class MultiValueFormatter implements TabularValueFormatter {

  @Override
  public String format(List<String> values) {
    StringBuilder valueRecord = new StringBuilder("[");
    valueRecord.append(values.stream().collect(Collectors.joining(",")));
    valueRecord.append("]");
    return valueRecord.toString();
  }

  @Override
  public String format(FormattedTabularRecordStreamer.ValueStream<String> stream,
                       long idIndex) {
    StringBuilder valueRecord = new StringBuilder("[");
    boolean first = true;
    while (stream.hasNext() && stream.peek().getIdIndex() == idIndex) {
      if (!first) {
        valueRecord.append(",");
      }
      valueRecord.append(stream.next().getValue());
      first = false;
    }
    valueRecord.append("]");
    return valueRecord.toString();
  }
}
