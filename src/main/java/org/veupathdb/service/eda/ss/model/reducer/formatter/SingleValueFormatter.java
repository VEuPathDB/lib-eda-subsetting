package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.reducer.FormattedTabularRecordStreamer;

import java.util.List;

public class SingleValueFormatter implements TabularValueFormatter {

  @Override
  public byte[] format(FormattedTabularRecordStreamer.ValueStream<byte[]> stream, long idIndex) {
    return stream.next().getValue();
  }
}
