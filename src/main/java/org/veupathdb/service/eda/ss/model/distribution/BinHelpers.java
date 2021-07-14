package org.veupathdb.service.eda.ss.model.distribution;

import java.time.LocalDateTime;
import org.veupathdb.service.eda.generated.model.HistogramBin;
import org.veupathdb.service.eda.generated.model.HistogramBinImpl;
import org.veupathdb.service.eda.ss.service.RequestBundle;

public class BinHelpers {

  public interface Bin<T> {
    boolean startsAfter(T value);
    boolean accept(T value, Long count);
    HistogramBin toHistogramBin();
  }

  public static class NumberBin implements Bin<Double> {

    public final Double _start;
    public final Double _end;

    private long _binCount = 0;

    public NumberBin(Double start, Double end) {
      _start = start;
      _end = end;
    }

    @Override
    public boolean startsAfter(Double value) {
      return (value < _start);
    }

    @Override
    public boolean accept(Double value, Long count) {
      // start inclusive, end exclusive
      if (value >= _start && value < _end) {
        _binCount += count;
        return true;
      }
      return false;
    }

    @Override
    public HistogramBin toHistogramBin() {
      HistogramBin bin = new HistogramBinImpl();
      bin.setBinStart(String.valueOf(_start));
      bin.setBinEnd(String.valueOf(_end));
      bin.setBinLabel("[" + bin.getBinStart() + "," + bin.getBinEnd() + ")");
      bin.setValue(_binCount);
      return bin;
    }
  }

  public static class DateBin implements Bin<LocalDateTime> {

    public final LocalDateTime _start;
    public final LocalDateTime _end;

    private long _binCount = 0;

    public DateBin(LocalDateTime start, LocalDateTime end) {
      _start = start;
      _end = end;
    }

    @Override
    public boolean startsAfter(LocalDateTime value) {
      return _start.isAfter(value);
    }

    @Override
    public boolean accept(LocalDateTime value, Long count) {
      // start inclusive, end exclusive
      if (!value.isBefore(_start) && value.isBefore(_end)) {
        _binCount += count;
        return true;
      }
      return false;
    }

    @Override
    public HistogramBin toHistogramBin() {
      HistogramBin bin = new HistogramBinImpl();
      bin.setBinStart(RequestBundle.formatDate(_start));
      bin.setBinEnd(RequestBundle.formatDate(_end));
      // TODO: fix label to be actual day, month, year (already accurate for week)
      bin.setBinLabel("[" + bin.getBinStart() + "," + bin.getBinEnd() + ")");
      bin.setValue(_binCount);
      return bin;
    }
  }
}