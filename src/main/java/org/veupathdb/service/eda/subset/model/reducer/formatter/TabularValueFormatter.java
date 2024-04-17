package org.veupathdb.service.eda.subset.model.reducer.formatter;

import org.veupathdb.service.eda.subset.model.reducer.ValueStream;

public interface TabularValueFormatter {

  byte[] format(ValueStream<byte[]> stream, long idIndex);

  String formatString(ValueStream<String> stream, long idIndex);
}
