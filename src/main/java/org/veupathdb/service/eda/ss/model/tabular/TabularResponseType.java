package org.veupathdb.service.eda.ss.model.tabular;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.function.Supplier;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import org.gusdb.fgputil.web.MimeTypes;
import org.json.JSONArray;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.gusdb.fgputil.FormatUtil.TAB;

public enum TabularResponseType {
  TABULAR(() -> new TabularFormatter(), MimeTypes.TEXT_TABULAR),
  JSON(() -> new JsonFormatter(), MediaType.APPLICATION_JSON);

  private final Supplier<Formatter> _formatter;
  private final String _mediaType;

  private TabularResponseType(Supplier<Formatter> formatter, String mediaType) {
    _formatter = formatter;
    _mediaType = mediaType;
  }

  public Formatter getFormatter() {
    return _formatter.get();
  }

  public String getMediaType() {
    return _mediaType;
  }

  public static TabularResponseType fromAcceptHeader(ContainerRequestContext request) {
    String header = request.getHeaders().getFirst(HttpHeaders.ACCEPT);
    return header == null || !JSON.getMediaType().equals(header)
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

