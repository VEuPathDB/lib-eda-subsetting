package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.reducer.FormattedTabularRecordStreamer;
import org.veupathdb.service.eda.ss.model.reducer.ValueStream;

public interface TabularValueFormatter {

  byte[] format(ValueStream<byte[]> stream, long idIndex);

  String formatString(ValueStream<String> stream, long idIndex);
}
