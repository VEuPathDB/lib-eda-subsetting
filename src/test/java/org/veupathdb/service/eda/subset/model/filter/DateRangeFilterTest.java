package org.veupathdb.service.eda.subset.model.filter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.variable.DateVariable;
import org.veupathdb.service.eda.subset.testutil.TestDataProvider;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

public class DateRangeFilterTest {

  @Test
  public void testEndDateInclusive() {
    final LocalDateTime dateTime = LocalDateTime.now();
    final LocalDateTime min = dateTime.minus(1, ChronoUnit.HOURS);
    final LocalDateTime max = dateTime;
    final Entity entity = TestDataProvider.constructEntity();
    final DateVariable dateVar = TestDataProvider.constructDateVariable(entity);
    entity.addVariable(dateVar);
    final DateRangeFilter filter = new DateRangeFilter("test", entity, dateVar, min, max);
    Assertions.assertTrue(filter.getPredicate().test(dateTime.toInstant(ZoneOffset.UTC).toEpochMilli()));
  }

  @Test
  public void testStartDateInclusive() {
    final LocalDateTime dateTime = LocalDateTime.now();
    final LocalDateTime min = dateTime;
    final LocalDateTime max = dateTime.plus(1, ChronoUnit.HOURS);
    final Entity entity = TestDataProvider.constructEntity();
    final DateVariable dateVar = TestDataProvider.constructDateVariable(entity);
    entity.addVariable(dateVar);
    final DateRangeFilter filter = new DateRangeFilter("test", entity, dateVar, min, max);
    Assertions.assertTrue(filter.getPredicate().test(dateTime.toInstant(ZoneOffset.UTC).toEpochMilli()));
  }
}
