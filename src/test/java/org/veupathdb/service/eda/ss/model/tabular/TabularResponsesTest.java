package org.veupathdb.service.eda.ss.model.tabular;

import org.json.JSONArray;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TabularResponsesTest {

  @Test
  public void testJsonBinaryFormatter() throws IOException {
    TabularResponses.BinaryFormatterFactory jsonFormatterFactory = TabularResponses.Type.JSON.getBinaryFormatter();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    TabularResponses.BinaryFormatter formatter =  jsonFormatterFactory.getFormatter(output);
    byte[][] input = new byte[5][5];
    input[0] = "abcde".getBytes(StandardCharsets.UTF_8);
    input[1] = "fghij".getBytes(StandardCharsets.UTF_8);
    input[2] = "klmno".getBytes(StandardCharsets.UTF_8);
    input[3] = "pqrst".getBytes(StandardCharsets.UTF_8);
    input[4] = "uvwxy".getBytes(StandardCharsets.UTF_8);
    formatter.begin();
    formatter.consumeRow(input);
    formatter.consumeRow(input);
    formatter.end();
    JSONArray arr = new JSONArray(output.toString());
    Assertions.assertEquals(2, arr.length());
    Assertions.assertEquals(arr.getJSONArray(0).length(), 5);
    Assertions.assertEquals(arr.getJSONArray(1).length(), 5);
  }

  @Test
  public void testJsonBinaryFormatterEscapeChars() throws IOException {
    TabularResponses.BinaryFormatterFactory jsonFormatterFactory = TabularResponses.Type.JSON.getBinaryFormatter();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    TabularResponses.BinaryFormatter formatter =  jsonFormatterFactory.getFormatter(output);
    byte[][] input = new byte[5][5];
    input[0] = "abcd\"".getBytes(StandardCharsets.UTF_8);
    input[1] = "fghis".getBytes(StandardCharsets.UTF_8);
    input[2] = "klmn/".getBytes(StandardCharsets.UTF_8);
    input[3] = "pqrst".getBytes(StandardCharsets.UTF_8);
    input[4] = "uvwxy".getBytes(StandardCharsets.UTF_8);
    formatter.begin();
    formatter.consumeRow(input);
    formatter.consumeRow(input);
    formatter.end();
    JSONArray arr = new JSONArray(output.toString());;
    Assertions.assertEquals(2, arr.length());
    Assertions.assertEquals(arr.getJSONArray(0).length(), 5);
    Assertions.assertEquals(arr.getJSONArray(1).length(), 5);
  }

}
