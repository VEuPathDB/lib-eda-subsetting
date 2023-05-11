package org.veupathdb.service.eda.ss.model.tabular;

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
    formatter.consumeRow(input);
    formatter.consumeRow(input);
    System.out.println(output);
    Assertions.assertEquals("[abcde,fghij,klmno,pqrst,uvwxy]\n[abcde,fghij,klmno,pqrst,uvwxy]\n", output.toString());
  }
}
