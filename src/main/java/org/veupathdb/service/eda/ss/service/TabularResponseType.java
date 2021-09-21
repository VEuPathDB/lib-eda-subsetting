package org.veupathdb.service.eda.ss.service;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.function.Supplier;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import org.json.JSONArray;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.gusdb.fgputil.FormatUtil.TAB;

public enum TabularResponseType {
  TABULAR(() -> new TabularFormatter()),
  JSON(() -> new JsonFormatter());

  private Supplier<Formatter> _formatter;

  private TabularResponseType(Supplier<Formatter> formatter) {
    _formatter = formatter;
  }

  public Formatter getFormatter() {
    return _formatter.get();
  }

  public static TabularResponseType fromAcceptHeader(ContainerRequestContext request) {
    String header = request.getHeaders().getFirst("Accept");
    return header == null || !MediaType.APPLICATION_JSON.equals(header)
      ? TABULAR : JSON;
  }

  public interface Formatter {
    default void begin(Writer writer) throws IOException {}
    void writeRow(Writer writer, List<String> values) throws IOException;
    default void end(Writer writer) throws IOException {}

  }

  private static class TabularFormatter implements Formatter {
    @Override
    public void writeRow(Writer writer, List<String> values) throws IOException {
      writer.write(String.join(TAB, values) + NL);
    }
  }

  private static class JsonFormatter implements Formatter {
    private boolean _firstWritten = false;
    @Override
    public void begin(Writer writer) throws IOException {
      writer.write('[');
    }
    @Override
    public void writeRow(Writer writer, List<String> values) throws IOException {
      if (_firstWritten) {
        writer.write(',');
      }
      else {
        _firstWritten = true;
      }
      writer.write(new JSONArray(values).toString());
    }
    @Override
    public void end(Writer writer) throws IOException {
      writer.write(']');
    }

  }
}
