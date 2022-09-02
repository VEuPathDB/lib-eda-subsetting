package org.veupathdb.service.eda.ss.testutil;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.NumberVariable;
import org.veupathdb.service.eda.ss.model.variable.StringVariable;
import org.veupathdb.service.eda.ss.model.variable.VariableType;

public class IndiaICEMRStudy {
  private Study study;
  private String studyAbbrev = "INDIABCS1_1";
  private Entity householdEntity;
  private Entity participantEntity;
  private Entity sampleEntity;
  private NumberVariable<Long> personsInHousehold;
  private NumberVariable<Long> timeSinceLastMalaria;
  private NumberVariable<Long> plasmoFalcGametocytes;
  private NumberVariable<Long> healthFacilityDist;
  private StringVariable householdMosquitoRepellent;
  private StringVariable householdMosquitoRepellentCoils;

  public IndiaICEMRStudy() {
    final String householdEntityId = "PCO_0000024";
    final String participantEntityId = "EUPATH_0000096";
    final String sampleEntityId = "EUPATH_0000609";
    householdEntity = new TestDataProvider.EntityBuilder()
        .withEntityId(householdEntityId)
        .withInternalStudyAbbrev(studyAbbrev)
        .build();
    householdMosquitoRepellent = new TestDataProvider.StringVariableBuilder()
        .withEntity(householdEntity)
        .withVariableType(VariableType.STRING)
        .withVariableId("EUPATH_0021242")
        .build();
    householdMosquitoRepellentCoils = new TestDataProvider.StringVariableBuilder()
        .withEntity(householdEntity)
        .withVariableType(VariableType.STRING)
        .withVariableId("EUPATH_0021243")
        .build();
    personsInHousehold =  new TestDataProvider.IntegerVariableBuilder()
        .withVariableId("EUPATH_0000019")
        .withEntity(householdEntity)
        .withVariableType(VariableType.INTEGER)
        .build();
    householdEntity.addVariable(personsInHousehold);
    householdEntity.addVariable(householdMosquitoRepellent);
    householdEntity.addVariable(householdMosquitoRepellentCoils);
    healthFacilityDist = new TestDataProvider.IntegerVariableBuilder()
        .withVariableId("EUPATH_0020213")
        .withEntity(householdEntity)
        .withVariableType(VariableType.INTEGER)
        .build();
    householdEntity.addVariable(healthFacilityDist);
    participantEntity = new TestDataProvider.EntityBuilder()
        .withEntityId(participantEntityId)
        .withInternalStudyAbbrev(studyAbbrev)
        .build();
    timeSinceLastMalaria = new TestDataProvider.IntegerVariableBuilder()
        .withVariableId("EUPATH_0000427")
        .withEntity(participantEntity)
        .withVariableType(VariableType.INTEGER)
        .build();
    participantEntity.addVariable(timeSinceLastMalaria);
    sampleEntity = new TestDataProvider.EntityBuilder()
        .withEntityId(sampleEntityId)
        .withInternalStudyAbbrev(studyAbbrev)
        .build();
    plasmoFalcGametocytes = new TestDataProvider.IntegerVariableBuilder()
        .withVariableId("EUPATH_0000546")
        .withEntity(sampleEntity)
        .withVariableType(VariableType.INTEGER)
        .build();
    sampleEntity.addVariable(plasmoFalcGametocytes);
    this.study = new TestDataProvider.StudyBuilder(studyAbbrev, studyAbbrev)
        .withRoot(householdEntity)
        .addEntity(participantEntity, householdEntity.getId())
        .addEntity(sampleEntity, participantEntity.getId())
        .build();
  }

  public Study getStudy() {
    return study;
  }

  public String getStudyAbbrev() {
    return studyAbbrev;
  }

  public Entity getHouseholdEntity() {
    return householdEntity;
  }

  public Entity getParticipantEntity() {
    return participantEntity;
  }

  public Entity getSampleEntity() {
    return sampleEntity;
  }

  public NumberVariable<Long> getPersonsInHousehold() {
    return personsInHousehold;
  }

  public NumberVariable<Long> getTimeSinceLastMalaria() {
    return timeSinceLastMalaria;
  }

  public NumberVariable<Long> getPlasmoFalcGametocytes() {
    return plasmoFalcGametocytes;
  }

  public NumberVariable<Long> getHealthFacilityDist() {
    return healthFacilityDist;
  }

  public StringVariable getHouseholdMosquitoRepellent() {
    return householdMosquitoRepellent;
  }

  public StringVariable getHouseholdMosquitoRepellentCoils() {
    return householdMosquitoRepellentCoils;
  }
}
