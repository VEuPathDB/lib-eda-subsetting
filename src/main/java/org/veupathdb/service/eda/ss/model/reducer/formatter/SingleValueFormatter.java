package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.reducer.FormattedTabularRecordStreamer;

import java.util.List;

public class SingleValueFormatter implements TabularValueFormatter {

  @Override
  public String format(List<String> values) {
    if (values.isEmpty()) {
      return "";
    }
    return values.get(0);
  }

  @Override
  public String format(FormattedTabularRecordStreamer.ValueStream<String> stream, long idIndex) {
    return stream.hasNext() ? stream.next().getValue() : "";
  }
}
