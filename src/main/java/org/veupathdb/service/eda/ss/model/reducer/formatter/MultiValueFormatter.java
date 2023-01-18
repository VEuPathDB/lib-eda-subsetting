package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.reducer.FormattedTabularRecordStreamer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class MultiValueFormatter implements TabularValueFormatter {

  @Override
  public byte[] format(List<byte[]> values) {
    final byte[] result = new byte[values.size() - 1 + values.stream().mapToInt(arr -> arr.length).sum() + 2];
    int index = 0;
    result[index++] = '['; // UTF 8?
    for (byte[] record: values) {
      for (int i = 0; i < record.length; i++) {
        result[index++] = record[i];
      }
      if (record != values.get(values.size() - 1)) {
        result[index++] = ',';
      }
    }
    result[index + 1] = ']';
    return result;
  }

  @Override
  public byte[] format(FormattedTabularRecordStreamer.ValueStream<byte[]> stream,
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
    return valueRecord.toString().getBytes(StandardCharsets.UTF_8);
  }
}
