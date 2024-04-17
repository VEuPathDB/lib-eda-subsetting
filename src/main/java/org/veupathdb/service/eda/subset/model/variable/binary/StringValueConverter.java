package org.veupathdb.service.eda.subset.model.variable.binary;

import org.gusdb.fgputil.FormatUtil;

import java.nio.ByteBuffer;

import static org.gusdb.fgputil.FormatUtil.decodeUtf8EncodedBytes;

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
  public String fromBytes(byte[] bytes, int offset) {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    buf.position(offset);
    return fromBytes(buf);
  }

  @Override
  public String fromBytes(ByteBuffer buffer) {
    int stringLength = buffer.getInt();
    byte[] stringBytes = new byte[stringLength];
    buffer.get(stringBytes, 0, stringLength);
    buffer.position(buffer.position() + (_numBytes - stringLength - 4));
    return decodeUtf8EncodedBytes(stringBytes);
  }

  @Override
  public int numBytes() {
    return _numBytes;
  }

}
