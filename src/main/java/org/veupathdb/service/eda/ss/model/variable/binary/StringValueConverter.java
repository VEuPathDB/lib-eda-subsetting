package org.veupathdb.service.eda.ss.model.variable.binary;

import org.gusdb.fgputil.FormatUtil;

public class StringValueConverter implements BinaryConverter<String> {

  private final int _numBytes;

  public StringValueConverter(int numBytes) {
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