package org.veupathdb.service.eda.ss.model.variable.serializer;

import org.gusdb.fgputil.FormatUtil;

public class StringValueSerializer implements ValueSerializer<String> {

  private final int _numBytes;

  public StringValueSerializer(int numBytes) {
    _numBytes = numBytes;
  }

  @Override
  public byte[] toBytes(String varValue) {
    return FormatUtil.stringToPaddedBinary(varValue, _numBytes);
  }

  @Override
  public String fromBytes(byte[] bytes) {
    return FormatUtil.paddedBinaryToString(bytes);
  }

  @Override
  public int numBytes() {
    return _numBytes;
  }

}
