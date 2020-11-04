package org.veupathdb.service.edass.model;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FiltersForTesting {

  // filters using data from the test db
  public Filter houseCityFilter;
  public Filter obsWeightFilter;
  public Filter partHairFilter;
  public Filter obsFavNumberFilter; // categorical numeric
  public Filter obsBirthDateFilter;  // continuous date
  public Filter obsVisitDateFilter;  // categorical numeric
  public Filter obsMoodFilter; // string
  public Filter houseObsWaterSupplyFilter; // string

  public FiltersForTesting(Study study) {

    Entity household = study.getEntity("GEMS_House").orElseThrow();
    Entity householdObs = study.getEntity("GEMS_HouseObs").orElseThrow();
    Entity participant = study.getEntity("GEMS_Part").orElseThrow();
    Entity observation = study.getEntity("GEMS_PartObs").orElseThrow();

    Variable city = household.getVariable("var-18").orElseThrow();
    Variable watersupply = householdObs.getVariable("var-19").orElseThrow();
    Variable haircolor = participant.getVariable("var-17").orElseThrow();
    Variable weight = observation.getVariable("var-12").orElseThrow();
    Variable favNumber = observation.getVariable("var-13").orElseThrow();
    Variable startDate = observation.getVariable("var-14").orElseThrow();
    Variable visitDate = observation.getVariable("var-15").orElseThrow();
    Variable mood = observation.getVariable("var-16").orElseThrow();

    List<String> haircolors = Arrays.asList(new String[]{"blond", "green"});
    partHairFilter = new StringSetFilter(participant, haircolor.getId(), haircolors);

    obsWeightFilter = new NumberRangeFilter(observation, weight.getId(), 10, 20);

    List<Number> favNums = Arrays.asList(new Number[]{5,7,9});
    obsFavNumberFilter = new NumberSetFilter(observation, favNumber.getId(), favNums);

    obsBirthDateFilter = new DateRangeFilter(observation, startDate.getId(),
        LocalDateTime.of(2019, Month.MARCH, 21, 00, 00),
        LocalDateTime.of(2019, Month.MARCH, 28, 00, 00));

    List<LocalDateTime> dates = new ArrayList<LocalDateTime>();
    dates.add(LocalDateTime.of(2019, Month.MARCH, 21, 00, 00));
    dates.add(LocalDateTime.of(2019, Month.MARCH, 28, 00, 00));
    dates.add(LocalDateTime.of(2019, Month.JUNE, 12, 00, 00));
    obsVisitDateFilter = new DateSetFilter(observation, visitDate.getId(), dates);

    List<String> moods = Arrays.asList(new String[]{"happy", "jolly", "giddy"});
    obsMoodFilter = new StringSetFilter(observation, mood.getId(), moods);

    obsWeightFilter = new NumberRangeFilter(observation, weight.getId(), 10, 20);

    List<String> cities = Arrays.asList(new String[]{"Boston"});
    houseCityFilter = new StringSetFilter(household, city.getId(), cities);

    List<String> waterSupplies = Arrays.asList(new String[]{"piped", "well"});
    houseObsWaterSupplyFilter = new StringSetFilter(householdObs, watersupply.getId(), waterSupplies);
  }
}
