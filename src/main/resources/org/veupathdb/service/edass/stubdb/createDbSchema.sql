-- the EDA service doesn't need to know much about the Study, because the WDK will serve that data
-- the abbrev would be used in the name of the tall and ancestors tables
-- the study_id is a stable ID

--apidb.Study :: one row per study (possibly many rows per investigation?)
--apidb.EntityTypeGraph  :: entity types + parent
--apidb.AttributeUnit  :: units for attributes/variables
--apidb.Attribute  :: Table describing the attributes which have values
--apidb.AttributeGraph  :: attribute/variable + parent
--apidb.attributevalue_$entityTypeId
--apidb.ancestors_$entityTypeId (edited) 

create table Study (
  study_id integer not null,
  name varchar(30) not null,
  PRIMARY KEY (study_id)
);

-- the entity_id is a stable id
CREATE TABLE EntityTypeGraph (
  entity_type_stable_id varchar(50) not null,
  entity_type_name varchar(30) not null,
  study_id integer not null,
  parent_entity_type_stable_id varchar(30),
  description varchar(100),
  abbrev varchar(20),
  PRIMARY KEY (entity_type_stable_id),
);
alter table EntityTypeGraph add unique (entity_type_name, study_id);
alter table EntityTypeGraph add unique (parent_entity_type_stable_id, study_id);
alter table EntityTypeGraph add unique (entity_type_stable_id, study_id);
ALTER TABLE EntityTypeGraph 
   ADD FOREIGN KEY (study_id) REFERENCES Study (study_id); 

--ATTRIBUTE_GRAPH_ID      NOT NULL NUMBER(12)     
--STUDY_ID                NOT NULL NUMBER(12)     
--ONTOLOGY_TERM_ID        NOT NULL NUMBER(10)     
--SOURCE_ID               NOT NULL VARCHAR2(255)  
--PARENT_SOURCE_ID        NOT NULL VARCHAR2(255)  
--PARENT_ONTOLOGY_TERM_ID NOT NULL NUMBER(10)     
--PROVIDER_LABEL                   VARCHAR2(30)   
--DISPLAY_NAME            NOT NULL VARCHAR2(1500) 
--TERM_TYPE                        VARCHAR2(20)   

create table AttributeGraph (
  stable_id varchar(30),
  ontology_term_id integer,
  study_id integer not null,
  parent_stable_id varchar(30),
  provider_label varchar(30) not null,
  display_name varchar(30) not null,
  term_type varchar(20),
  PRIMARY KEY (stable_id)
);
ALTER TABLE AttributeGraph 
   ADD FOREIGN KEY (study_id) REFERENCES Study (study_id);
ALTER TABLE AttributeGraph 
   ADD FOREIGN KEY (parent_stable_id) REFERENCES AttributeGraph (stable_id); 

   
-- this is the brief version of the ontology tree that is needed by EDA
-- a "variable" might have values or not.  if not, it is just a category.
-- the "variable_id" would hold the ontology term (ie, is a stable ID)
-- we might want to add an variable_id (integer) for performance reasons?

--ATTRIBUTE_ID                   NOT NULL NUMBER(12)    
--ENTITY_TYPE_ID                          NUMBER(12)    
--PROCESS_TYPE_ID                         NUMBER(12)    
--ONTOLOGY_TERM_ID               NOT NULL NUMBER(10)    
--SOURCE_ID                      NOT NULL VARCHAR2(255) 
--DATA_TYPE                      NOT NULL VARCHAR2(10)  
--HAS_MULTIPLE_VALUES_PER_ENTITY          NUMBER(38)    
--DATA_SHAPE                              VARCHAR2(30)  
--UNIT                                    VARCHAR2(30)  
--UNIT_ONTOLOGY_TERM_ID                   NUMBER(10)    
--PRECISION                               NUMBER(38) 

create table Attribute (
  stable_id varchar(30),
  ontology_term_id integer,
  entity_type_stable_id varchar(30),
  has_multiple_values_per_entity integer,
  data_shape varchar(10),
  unit varchar (30),
  unit_ontology_term_id integer,
  precision integer,
  PRIMARY KEY (stable_id)
);
ALTER TABLE Attribute 
   ADD FOREIGN KEY (entity_type_stable_id) REFERENCES EntityTypeGraph (entity_type_stable_id);
  
--ENTITY_ID                  NOT NULL NUMBER(12)     
--ATTRIBUTE_ONTOLOGY_TERM_ID NOT NULL NUMBER(10)     
--STRING_VALUE                        VARCHAR2(1000) 
--NUMBER_VALUE                        NUMBER         
--DATE_VALUE                          DATE           

create table AttributeValue_1000_Prtcpnt (
  prtcpnt_id integer,
  ontology_term_id integer,
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
  PRIMARY KEY (entity_id, ontology_term_id)
);

ALTER TABLE AttributeValue_1000_Hshld 
   ADD FOREIGN KEY (ontology_term_id) REFERENCES Attribute (ontology_term_id); 
CREATE UNIQUE INDEX AttributeValue_1000_Hshld_i1
ON AttributeValue_1000_Hshld (ontology_term_id, number_value, entity_id);
CREATE UNIQUE INDEX AttributeValue_1000_Hshld_i2
ON AttributeValue_1000_Hshld (ontology_term_id, string_value, entity_id);
CREATE UNIQUE INDEX AttributeValue_1000_Hshld_i3
ON AttributeValue_1000_Hshld (ontology_term_id, date_value, entity_id);
   
create table Ancestors_1000_Prtcpnt (
  prtcpnt_id integer,
  hshld_id integer,
  PRIMARY KEY (prtcpnt_id)
);
CREATE UNIQUE INDEX GEMS_House_ancestors_i1
ON GEMS_House_ancestors (GEMS_House_id);


   
-------------------------------------------------------------------------------------   
-- THE FOLLOWING TABLES ARE AN EXAMPLE OF THE TABLES FOR A PARTICULAR FAKE STUDY (GEMS)
-------------------------------------------------------------------------------------   
-- might want to use an integer internal_variable_id for performance
create table GEMS_House_tall (
  GEMS_House_id integer,
  variable_id varchar(30),
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
  PRIMARY KEY (GEMS_House_id)
);
ALTER TABLE GEMS_House_tall 
   ADD FOREIGN KEY (variable_id) REFERENCES Variable (variable_id); 
CREATE UNIQUE INDEX GEMS_House_tall_i1
ON GEMS_House_tall (variable_id, number_value, GEMS_House_id);
CREATE UNIQUE INDEX GEMS_House_tall_i2
ON GEMS_House_tall (variable_id, string_value, GEMS_House_id);
CREATE UNIQUE INDEX GEMS_House_tall_i3
ON GEMS_House_tall (variable_id, date_value, GEMS_House_id);

create table GEMS_House_ancestors (
  GEMS_House_id integer,
  PRIMARY KEY (GEMS_House_id)
);
CREATE UNIQUE INDEX GEMS_House_ancestors_i1
ON GEMS_House_ancestors (GEMS_House_id);

create table GEMS_HouseObs_tall (
  GEMS_HouseObs_id integer,
  variable_id varchar(30),
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
  PRIMARY KEY (GEMS_HouseObs_id)
);
ALTER TABLE GEMS_HouseObs_tall 
   ADD FOREIGN KEY (variable_id) REFERENCES Variable (variable_id); 
CREATE UNIQUE INDEX GEMS_HouseObs_tall_i1
ON GEMS_HouseObs_tall (variable_id, number_value, GEMS_HouseObs_id);
CREATE UNIQUE INDEX GEMS_HouseObs_tall_i2
ON GEMS_HouseObs_tall (variable_id, string_value, GEMS_HouseObs_id);
CREATE UNIQUE INDEX GEMS_HouseObs_tall_i3
ON GEMS_HouseObs_tall (variable_id, date_value, GEMS_HouseObs_id);

create table GEMS_HouseObs_ancestors (
  GEMS_HouseObs_id integer,
  GEMS_House_id integer,
);
CREATE UNIQUE INDEX GEMS_HouseObs_ancestors_i1
ON GEMS_HouseObs_ancestors (GEMS_HouseObs_id);

create table GEMS_Part_tall (
  GEMS_Part_id integer,
  variable_id varchar(30),
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
);
ALTER TABLE GEMS_Part_tall 
   ADD FOREIGN KEY (variable_id) REFERENCES Variable (variable_id); 
CREATE UNIQUE INDEX GEMS_Part_tall_i1
ON GEMS_Part_tall (variable_id, number_value, GEMS_Part_id);
CREATE UNIQUE INDEX GEMS_Part_tall_i2
ON GEMS_Part_tall (variable_id, string_value, GEMS_Part_id);
CREATE UNIQUE INDEX GEMS_Part_tall_i3
ON GEMS_Part_tall (variable_id, date_value, GEMS_Part_id);

create table GEMS_Part_ancestors (
  GEMS_Part_id integer,
  GEMS_House_id integer, 
  PRIMARY KEY (GEMS_Part_id)
);
CREATE UNIQUE INDEX GEMS_Part_ancestors_i1
ON GEMS_Part_ancestors (GEMS_Part_id);

create table GEMS_PartObs_tall (
  GEMS_PartObs_id integer,
  variable_id varchar(30),
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
);
ALTER TABLE GEMS_PartObs_tall 
   ADD FOREIGN KEY (variable_id) REFERENCES Variable (variable_id); 
CREATE UNIQUE INDEX GEMS_PartObs_tall_i1
ON GEMS_PartObs_tall (variable_id, number_value, GEMS_PartObs_id);
CREATE UNIQUE INDEX GEMS_PartObs_tall_i2
ON GEMS_PartObs_tall (variable_id, string_value, GEMS_PartObs_id);
CREATE UNIQUE INDEX GEMS_PartObs_tall_i3
ON GEMS_PartObs_tall (variable_id, date_value, GEMS_PartObs_id);

create table GEMS_PartObs_ancestors (
  GEMS_PartObs_id integer,
  GEMS_Part_id integer,
  GEMS_House_id integer, 
  PRIMARY KEY (GEMS_PartObs_id)
);
CREATE UNIQUE INDEX GEMS_PartObs_ancestors_i1
ON GEMS_PartObs_ancestors (GEMS_PartObs_id);



