package org.veupathdb.service.eda.subset.testutil;

import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.variable.DateVariable;
import org.veupathdb.service.eda.subset.model.variable.NumberVariable;
import org.veupathdb.service.eda.subset.model.variable.StringVariable;
import org.veupathdb.service.eda.subset.model.variable.VariableType;

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
  private StringVariable householdMosquitoRepellentMats;
  private StringVariable symptoms;

  private DateVariable observationDate;

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
        .withMaxLength(7)
        .build();
    householdMosquitoRepellentMats = new TestDataProvider.StringVariableBuilder()
        .withEntity(householdEntity)
        .withVariableType(VariableType.STRING)
        .withVariableId("EUPATH_0021246")
        .withMaxLength(7)
        .build();

    personsInHousehold =  new TestDataProvider.IntegerVariableBuilder()
        .withVariableId("EUPATH_0000019")
        .withEntity(householdEntity)
        .withVariableType(VariableType.INTEGER)
        .withMin(1)
        .withMax(50)
        .build();
    householdEntity.addVariable(personsInHousehold);
    householdEntity.addVariable(householdMosquitoRepellent);
    householdEntity.addVariable(householdMosquitoRepellentCoils);
    householdEntity.addVariable(householdMosquitoRepellentMats);
    healthFacilityDist = new TestDataProvider.IntegerVariableBuilder()
        .withVariableId("EUPATH_0020213")
        .withEntity(householdEntity)
        .withVariableType(VariableType.INTEGER)
        .withMin(1)
        .withMax(13)
        .build();
    householdEntity.addVariable(healthFacilityDist);
    participantEntity = new TestDataProvider.EntityBuilder()
        .withEntityId(participantEntityId)
        .withInternalStudyAbbrev(studyAbbrev)
        .build();
    observationDate = new TestDataProvider.DateVariableBuilder()
        .withVariableType(VariableType.DATE)
        .withEntity(participantEntity)
        .withVariableId("EUPATH_0004991")
        .build();
    timeSinceLastMalaria = new TestDataProvider.IntegerVariableBuilder()
        .withVariableId("EUPATH_0000427")
        .withEntity(participantEntity)
        .withVariableType(VariableType.INTEGER)
        .withMin(1)
        .withMax(360)
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
        .withMin(0)
        .withMax(200)
        .build();
    sampleEntity.addVariable(plasmoFalcGametocytes);
    symptoms = new TestDataProvider.StringVariableBuilder()
        .withEntity(participantEntity)
        .withVariableId("EUPATH_0021002")
        .withVariableType(VariableType.STRING)
        .withMaxLength(30)
        .build();
    participantEntity.addVariable(symptoms);
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

  public StringVariable getHouseholdMosquitoRepellentMats() {
    return householdMosquitoRepellentMats;
  }

  public StringVariable getSymptoms() {
    return symptoms;
  }

  public DateVariable getObservationDate() {
    return observationDate;
  }
}
