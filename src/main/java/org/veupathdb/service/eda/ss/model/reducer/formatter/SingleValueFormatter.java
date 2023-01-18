package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.reducer.FormattedTabularRecordStreamer;

import java.util.List;

public class SingleValueFormatter implements TabularValueFormatter {
  private static final byte[] EMPTY = new byte[0];

  @Override
  public byte[] format(List<byte[]> values) {
    if (values.isEmpty()) {
      return EMPTY;
    }
    return values.get(0);
  }

  @Override
  public byte[] format(FormattedTabularRecordStreamer.ValueStream<byte[]> stream, long idIndex) {
    return stream.next().getValue();
  }
}
