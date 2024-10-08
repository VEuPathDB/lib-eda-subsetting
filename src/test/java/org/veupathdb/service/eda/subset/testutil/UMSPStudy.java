package org.veupathdb.service.eda.subset.testutil;

import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.variable.DateVariable;
import org.veupathdb.service.eda.subset.model.variable.NumberVariable;
import org.veupathdb.service.eda.subset.model.variable.StringVariable;
import org.veupathdb.service.eda.subset.model.variable.VariableType;

public class UMSPStudy {
  private final Entity healthCenterEntity;
  private final Entity clinicVisitEntity;
  private final Entity sample;
  private final String studyAbbrev = "UMSP_1";

  private final NumberVariable<?> age;
  private final StringVariable nonMalarial;
  private final DateVariable observationDate;
  private final StringVariable plasmodium;
  private final StringVariable country;

  private final Study study;

  public UMSPStudy() {
    final String healthCenterEntityId = "PCO_0000024";
    final String clinicVisitEntityId = "EUPATH_0000096";
    final String sampleId = "EUPATH_0000609";
    sample = new TestDataProvider.EntityBuilder()
      .withEntityId(sampleId)
      .withInternalStudyAbbrev(studyAbbrev)
      .build();
    healthCenterEntity = new TestDataProvider.EntityBuilder()
      .withEntityId(healthCenterEntityId)
      .withInternalStudyAbbrev(studyAbbrev)
      .build();
    clinicVisitEntity = new TestDataProvider.EntityBuilder()
      .withEntityId(clinicVisitEntityId)
      .withInternalStudyAbbrev(studyAbbrev)
      .build();

    age = new TestDataProvider.IntegerVariableBuilder()
      .withEntity(this.clinicVisitEntity)
      .withVariableId("OBI_0001169")
      .withVariableType(VariableType.INTEGER)
      .build();
    nonMalarial = new TestDataProvider.StringVariableBuilder()
      .withEntity(this.clinicVisitEntity)
      .withVariableId("EUPATH_0000059")
      .withVariableType(VariableType.STRING)
      .withMaxLength(164)
      .build();

    observationDate = new TestDataProvider.DateVariableBuilder()
      .withEntity(this.clinicVisitEntity)
      .withVariableId("EUPATH_0004991")
      .withVariableType(VariableType.DATE)
      .build();
    plasmodium = new TestDataProvider.StringVariableBuilder()
      .withEntity(sample)
      .withVariableId("EUPATH_0033244")
      .withVariableType(VariableType.STRING)
      .withMaxLength(18)
      .build();
    country = new TestDataProvider.StringVariableBuilder()
      .withEntity(healthCenterEntity)
      .withVariableId("ENVO_00000009")
      .withVariableType(VariableType.STRING)
      .withMaxLength(10)
      .build();
    clinicVisitEntity.addVariable(age);
    clinicVisitEntity.addVariable(observationDate);
    clinicVisitEntity.addVariable(nonMalarial);
    sample.addVariable(plasmodium);
    healthCenterEntity.addVariable(country);

    study = new TestDataProvider.StudyBuilder(studyAbbrev, studyAbbrev)
      .withRoot(healthCenterEntity)
      .addEntity(clinicVisitEntity, healthCenterEntityId)
      .addEntity(sample, clinicVisitEntityId)
      .build();
  }

  public Entity getHealthCenterEntity() {
    return healthCenterEntity;
  }

  public Entity getClinicVisitEntity() {
    return clinicVisitEntity;
  }

  public String getStudyAbbrev() {
    return studyAbbrev;
  }

  public NumberVariable getAge() {
    return age;
  }

  public DateVariable getObservationDate() {
    return observationDate;
  }

  public StringVariable getPlasmodium() {
    return plasmodium;
  }

  public StringVariable getCountry() {
    return country;
  }

  public Study getStudy() {
    return study;
  }

  public Entity getSample() {
    return sample;
  }

  public StringVariable getNonMalarial() {
    return nonMalarial;
  }
}
