package org.veupathdb.service.eda.ss.model.tabular;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
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
    TABULAR(TABULAR_FORMATTER, MimeTypes.TEXT_TABULAR, TABULAR_BINARY_FORMATTER),
    JSON(JSON_FORMATTER, MediaType.APPLICATION_JSON, TABULAR_BINARY_FORMATTER);

    private final FormatterFactory _formatter;
    private final BinaryFormatterFactory _binaryFormatter;
    private final String _mediaType;

    Type(FormatterFactory formatter, String mediaType, BinaryFormatterFactory binaryFormatterFactory) {
      _formatter = formatter;
      _mediaType = mediaType;
      _binaryFormatter = binaryFormatterFactory;
    }

    public FormatterFactory getFormatter() {
      return _formatter;
    }

    public BinaryFormatterFactory getBinaryFormatter() {
      return _binaryFormatter;
    }

    public String getMediaType() {
      return _mediaType;
    }

    public static Type fromAcceptHeader(ContainerRequestContext request) {
      String header = request.getHeaders().getFirst(HttpHeaders.ACCEPT);
      return header == null || !JSON.getMediaType().equals(header)
          ? TABULAR : JSON;
    }
  }

  public interface ResultConsumer {
    default void begin() throws IOException {}
    void consumeRow(List<String> values) throws IOException;
    default void end() throws IOException {}
  }

  public interface BinaryResultConsumer {
    default void begin() throws IOException {}
    void consumeRow(byte[][] values) throws IOException;
    default void end() throws IOException {}
  }


  public static abstract class Formatter implements ResultConsumer {
    protected final Writer _writer;
    protected Formatter(Writer writer) {
      _writer = writer;
    }
  }

  public static abstract class BinaryFormatter implements BinaryResultConsumer {
    protected final OutputStream _outputStream;
    protected BinaryFormatter(OutputStream outputStream) {
      _outputStream = outputStream;
    }
  }


  @FunctionalInterface
  public interface FormatterFactory {
    Formatter getFormatter(Writer writer);
  }

  @FunctionalInterface
  public interface BinaryFormatterFactory {
    BinaryFormatter getFormatter(OutputStream writer);
  }


  private static final FormatterFactory TABULAR_FORMATTER = writer -> new Formatter(writer) {
    @Override
    public void consumeRow(List<String> values) throws IOException {
      _writer.write(String.join(TAB, values) + NL);
    }

  };

  private static final BinaryFormatterFactory TABULAR_BINARY_FORMATTER = outStream -> new BinaryFormatter(outStream) {

    @Override
    public void consumeRow(byte[][] values) throws IOException {
      for (int i = 0; i < values.length; i++) {
        outStream.write(values[i]);
        if (i != values.length - 1) {
          outStream.write('\t');
        }
      }
      outStream.write(System.lineSeparator().getBytes(StandardCharsets.UTF_8));
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

