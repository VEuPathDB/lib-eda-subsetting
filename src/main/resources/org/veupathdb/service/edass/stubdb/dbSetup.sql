-- the EDA service doesn't need to know much about the Study, because the WDK will serve that data
-- the abbrev would be used in the name of the tall and ancestors tables
create table Study (
  study_id integer not null,
  abbrev varchar(20) not null,
  PRIMARY KEY (study_id)
);

-- a controlled vocab of entity names, eg "Household", "Participant"
-- the abbrev would be used in the name of the tall and ancestors tables
CREATE TABLE EntityName (
  entity_name_id integer not null,
  name varchar(30) not null,
  plural_name varchar (30) not null,
  abbrev varchar(25),
  PRIMARY KEY (entity_name_id)
);

CREATE TABLE Entity (
  entity_id integer not null,
  entity_name_id integer not null,
  study_id integer not null,
  parent_entity_id integer,
  description varchar(25),
  PRIMARY KEY (entity_id)
);
ALTER TABLE Entity 
   ADD FOREIGN KEY (entity_name_id) REFERENCES EntityName (entity_name_id);
ALTER TABLE Entity 
   ADD FOREIGN KEY (study_id) REFERENCES Study (study_id); 
   
-- a controlled vocabulary of variable types, eg "string", "date", "number"
create table VariableType (
  variable_type_id integer not null,
  variable_type varchar(20) not null,
  PRIMARY KEY (variable_type_id)
);

create table Variable (
  variable_id integer not null,
  variable_type_id integer not null,
  entity_id integer not null,
  parent_variable_id integer,
  display_name varchar(30),
  is_continuous integer,
  units varchar (30),
  precision integer,
  PRIMARY KEY (variable_id)
);

ALTER TABLE Variable 
   ADD FOREIGN KEY (entity_id) REFERENCES Entity (entity_id);
ALTER TABLE Variable 
   ADD FOREIGN KEY (variable_type_id) REFERENCES VariableType (variable_type_id); 

create table GEMS_Household_tall (
  Household_id integer,
  variable_id integer,
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
  PRIMARY KEY (Household_id)
);
CREATE UNIQUE INDEX GEMS_Household_tall_i1
ON GEMS_Household_tall (variable_id, number_value, Household_id);
CREATE UNIQUE INDEX GEMS_Household_tall_i2
ON GEMS_Household_tall (variable_id, string_value, Household_id);
CREATE UNIQUE INDEX GEMS_Household_tall_i3
ON GEMS_Household_tall (variable_id, date_value, Household_id);

create table GEMS_Household_ancestors (
  Household_id integer,
  PRIMARY KEY (Household_id)
);
CREATE UNIQUE INDEX GEMS_Household_ancestors_i1
ON GEMS_Household_ancestors (Household_id);

create table GEMS_HouseholdObs_tall (
  HouseholdObs_id integer,
  variable_id integer,
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
  PRIMARY KEY (HouseholdObs_id)
);
CREATE UNIQUE INDEX GEMS_HouseholdObs_tall_i1
ON GEMS_HouseholdObs_tall (variable_id, number_value, HouseholdObs_id);
CREATE UNIQUE INDEX GEMS_HouseholdObs_tall_i2
ON GEMS_HouseholdObs_tall (variable_id, string_value, HouseholdObs_id);
CREATE UNIQUE INDEX GEMS_HouseholdObs_tall_i3
ON GEMS_HouseholdObs_tall (variable_id, date_value, HouseholdObs_id);

create table GEMS_HouseholdObs_ancestors (
  HouseholdObs_id integer,
  Household_id integer,
  PRIMARY KEY (HouseholdObs_id)
);
CREATE UNIQUE INDEX GEMS_HouseholdObs_ancestors_i1
ON GEMS_HouseholdObs_ancestors (HouseholdObs_id);

create table GEMS_Participant_tall (
  Participant_id integer,
  variable_id integer,
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
  PRIMARY KEY (Participant_id)
);
CREATE UNIQUE INDEX GEMS_Participant_tall_i1
ON GEMS_Participant_tall (variable_id, number_value, Participant_id);
CREATE UNIQUE INDEX GEMS_Participant_tall_i2
ON GEMS_Participant_tall (variable_id, string_value, Participant_id);
CREATE UNIQUE INDEX GEMS_Participant_tall_i3
ON GEMS_Participant_tall (variable_id, date_value, Participant_id);

create table GEMS_Participant_ancestors (
  Participant_id integer,
  Household_id integer, 
  PRIMARY KEY (Participant_id)
);
CREATE UNIQUE INDEX GEMS_Participant_ancestors_i1
ON GEMS_Participant_ancestors (Participant_id);

create table GEMS_ParticipantObs_tall (
  ParticipantObs_id integer,
  variable_id integer,
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
  PRIMARY KEY (ParticipantObs_id)
);
CREATE UNIQUE INDEX GEMS_ParticipantObs_tall_i1
ON GEMS_ParticipantObs_tall (variable_id, number_value, ParticipantObs_id);
CREATE UNIQUE INDEX GEMS_ParticipantObs_tall_i2
ON GEMS_ParticipantObs_tall (variable_id, string_value, ParticipantObs_id);
CREATE UNIQUE INDEX GEMS_ParticipantObs_tall_i3
ON GEMS_ParticipantObs_tall (variable_id, date_value, ParticipantObs_id);

create table GEMS_ParticipantObs_ancestors (
  ParticipantObs_id integer,
  Participant_id integer,
  Household_id integer, 
  PRIMARY KEY (ParticipantObs_id)
);
CREATE UNIQUE INDEX GEMS_ParticipantObs_ancestors_i1
ON GEMS_ParticipantObs_ancestors (ParticipantObs_id);

--INSERT INTO ENTITIES values (1, 'households', null);
--INSERT INTO ENTITIES values (2, 'participants', 1);
--INSERT INTO ENTITIES values (3, 'ParticipantObss', 2);


