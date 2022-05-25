package org.veupathdb.service.eda.ss.test;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.DateRangeFilter;
import org.veupathdb.service.eda.ss.model.filter.DateSetFilter;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.model.filter.NumberRangeFilter;
import org.veupathdb.service.eda.ss.model.filter.NumberSetFilter;
import org.veupathdb.service.eda.ss.model.filter.StringSetFilter;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;
import org.veupathdb.service.eda.ss.model.variable.NumberVariable;
import org.veupathdb.service.eda.ss.model.variable.StringVariable;

import static org.veupathdb.service.eda.ss.test.StubDb.APP_DB_SCHEMA;

public class MockFilters {

  // filters using data from the test db
  public Filter houseCityFilter;
  public Filter houseRoofFilter;
  public Filter obsWeightFilter;
  public Filter partHairFilter;
  public Filter obsFavNumberFilter; // categorical numeric
  public Filter obsBirthDateFilter;  // continuous date
  public Filter obsVisitDateFilter;  // categorical numeric
  public Filter obsFavNewYearsFilter;
  public Filter obsMoodFilter; // string
  public Filter houseObsWaterSupplyFilter; // string

  public MockFilters(Study study) {

    Entity household = study.getEntity("GEMS_House").orElseThrow();
    Entity householdObs = study.getEntity("GEMS_HouseObs").orElseThrow();
    Entity participant = study.getEntity("GEMS_Part").orElseThrow();
    Entity observation = study.getEntity("GEMS_PartObs").orElseThrow();

    StringVariable city = StringVariable.assertType(household.getVariable("var_h1").orElseThrow());
    StringVariable roof = StringVariable.assertType(household.getVariable("var_h2").orElseThrow());
    StringVariable watersupply = StringVariable.assertType(householdObs.getVariable("var_ho1").orElseThrow());
    StringVariable haircolor = StringVariable.assertType(participant.getVariable("var_p4").orElseThrow());
    StringVariable mood = StringVariable.assertType(observation.getVariable("var_o5").orElseThrow());
    NumberVariable weight = NumberVariable.assertType(observation.getVariable("var_o1").orElseThrow());
    NumberVariable favNumber = NumberVariable.assertType(observation.getVariable("var_o2").orElseThrow());
    DateVariable startDate = DateVariable.assertType(observation.getVariable("var_o3").orElseThrow());
    DateVariable visitDate = DateVariable.assertType(observation.getVariable("var_o4").orElseThrow());

    List<String> haircolors = Arrays.asList("blond");
    partHairFilter = new StringSetFilter(APP_DB_SCHEMA, participant, haircolor, haircolors);

    obsWeightFilter = new NumberRangeFilter(APP_DB_SCHEMA, observation, weight, 10, 20);

    List<Number> favNums = Arrays.asList(new Number[]{5,7,9});
    obsFavNumberFilter = new NumberSetFilter(APP_DB_SCHEMA, observation, favNumber, favNums);

    obsBirthDateFilter = new DateRangeFilter(APP_DB_SCHEMA, observation, startDate,
        LocalDateTime.of(2019, Month.MARCH, 21, 0, 0),
        LocalDateTime.of(2019, Month.MARCH, 28, 0, 0));

    List<LocalDateTime> dates = new ArrayList<>();
    dates.add(LocalDateTime.of(2019, Month.MARCH, 21, 0, 0));
    dates.add(LocalDateTime.of(2019, Month.MARCH, 28, 0, 0));
    dates.add(LocalDateTime.of(2019, Month.JUNE, 12, 0, 0));
    obsVisitDateFilter = new DateSetFilter(APP_DB_SCHEMA, observation, visitDate, dates);
    obsFavNewYearsFilter = new DateSetFilter(APP_DB_SCHEMA, observation, startDate, dates);

    List<String> moods = Arrays.asList("happy", "jolly", "giddy");
    obsMoodFilter = new StringSetFilter(APP_DB_SCHEMA, observation, mood, moods);

    obsWeightFilter = new NumberRangeFilter(APP_DB_SCHEMA, observation, weight, 10, 20);

    List<String> cities = Collections.singletonList("Boston");
    houseCityFilter = new StringSetFilter(APP_DB_SCHEMA, household, city, cities);

    List<String> waterSupplies = Arrays.asList("piped", "well");
    houseObsWaterSupplyFilter = new StringSetFilter(APP_DB_SCHEMA, householdObs, watersupply, waterSupplies);

    List<String> roofs = Arrays.asList("metal", "tile");
    houseRoofFilter = new StringSetFilter(APP_DB_SCHEMA, household, roof, roofs);
  }

}
