package org.veupathdb.service.eda.ss.model.tabular;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import org.gusdb.fgputil.web.MimeTypes;
import org.json.JSONArray;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.gusdb.fgputil.FormatUtil.TAB;

public class TabularResponses {
  private TabularResponses(){}

  public enum Type {
    TABULAR(TABULAR_FORMATTER, MimeTypes.TEXT_TABULAR),
    JSON(JSON_FORMATTER, MediaType.APPLICATION_JSON);

    private final FormatterFactory _formatter;
    private final String _mediaType;

    private Type(FormatterFactory formatter, String mediaType) {
      _formatter = formatter;
      _mediaType = mediaType;
    }

    public FormatterFactory getFormatter() {
      return _formatter;
    }

    public String getMediaType() {
      return _mediaType;
    }
  }

  public static Type fromAcceptHeader(ContainerRequestContext request) {
    String header = request.getHeaders().getFirst(HttpHeaders.ACCEPT);
    return header == null || !Type.JSON.getMediaType().equals(header)
        ? Type.TABULAR : Type.JSON;
  }

  public interface ResultConsumer {
    default void begin() throws IOException {}
    void consumeRow(List<String> values) throws IOException;
    default void end() throws IOException {}
  }

  public static abstract class Formatter implements ResultConsumer {
    protected final Writer _writer;
    protected Formatter(Writer writer) {
      _writer = writer;
    }
  }

  @FunctionalInterface
  public interface FormatterFactory {
    Formatter getFormatter(Writer writer);
  }

  private static final FormatterFactory TABULAR_FORMATTER = writer -> new Formatter(writer) {
    @Override
    public void consumeRow(List<String> values) throws IOException {
      _writer.write(String.join(TAB, values) + NL);
    }
  };

  private static final FormatterFactory JSON_FORMATTER = writer -> new Formatter(writer) {
    private boolean _firstWritten = false;
    @Override
    public void begin() throws IOException {
      _writer.write('[');
    }
    @Override
    public void consumeRow(List<String> values) throws IOException {
      if (_firstWritten) {
        _writer.write(',');
      }
      else {
        _firstWritten = true;
      }
      _writer.write(new JSONArray(values).toString());
    }
    @Override
    public void end() throws IOException {
      _writer.write(']');
    }
  };
}

