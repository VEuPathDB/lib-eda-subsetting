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
alter table AttributeGraph add unique (ontology_term_id);
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

-------------------------------------------------------------------------------------   
-- THE FOLLOWING TABLES ARE AN EXAMPLE OF THE TABLES FOR A PARTICULAR FAKE STUDY (GEMS)
-------------------------------------------------------------------------------------

--ENTITY_ID                  NOT NULL NUMBER(12)
--ATTRIBUTE_ONTOLOGY_TERM_ID NOT NULL NUMBER(10)
--STRING_VALUE                        VARCHAR2(1000)
--NUMBER_VALUE                        NUMBER
--DATE_VALUE                          DATE

create table AttrVal_1000_Hshld (
  hshld_id integer,
  ontology_term_id integer,
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
);
ALTER TABLE AttrVal_1000_Hshld
   ADD FOREIGN KEY (ontology_term_id) REFERENCES AttributeGraph (ontology_term_id);
CREATE INDEX AttrVal_1000_Hshld_i1
ON AttrVal_1000_Hshld (ontology_term_id, hshld_id, number_value, string_value, date_value);
-- for test db only
CREATE unique INDEX AttrVal_1000_Hshld_i2 ON AttrVal_1000_Hshld (ontology_term_id, hshld_id);

create table Ancestors_1000_Hshld (
  hshld_id integer,
  PRIMARY KEY (Hshld_id)
);

create table AttrVal_1000_HshldObsrvtn (
  HshldObsrvtn_id integer,
  ontology_term_id integer,
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
);
ALTER TABLE AttrVal_1000_HshldObsrvtn
   ADD FOREIGN KEY (ontology_term_id) REFERENCES AttributeGraph (ontology_term_id);
CREATE INDEX AttrVal_1000_HshldObsrvtn_i1
ON AttrVal_1000_HshldObsrvtn (ontology_term_id, HshldObsrvtn_id, number_value, string_value, date_value);
-- for test db only
CREATE unique INDEX AttrVal_1000_HshldObsrvtn_i2 ON AttrVal_1000_HshldObsrvtn (ontology_term_id, HshldObsrvtn_id);

create table Ancestors_1000_HshldObsrvtn (
  HshldObsrvtn_id integer,
  Hshld_id integer,
  PRIMARY KEY (Hshld_id)

);

create table AttrVal_1000_Prtcpnt (
  prtcpnt_id integer,
  ontology_term_id integer,
  number_value integer,
  string_value varchar(100),
  date_value varchar(30),
);
ALTER TABLE AttrVal_1000_Prtcpnt
   ADD FOREIGN KEY (ontology_term_id) REFERENCES AttributeGraph (ontology_term_id);
CREATE INDEX AttrVal_1000_Prtcpnt_i1
ON AttrVal_1000_Prtcpnt (ontology_term_id, prtcpnt_id, number_value, string_value, date_value);
-- for test db only
CREATE unique INDEX AttrVal_1000_Prtcpnt_i2 ON AttrVal_1000_Prtcpnt (ontology_term_id, prtcpnt_id);

create table Ancestors_1000_Prtcpnt (
  prtcpnt_id integer,
  hshld_id integer,
  PRIMARY KEY (prtcpnt_id)
);

create table AttrVal_1000_PrtcpntObsrvtn (
  PrtcpntObsrvtn_id integer,
  ontology_term_id integer,
  number_value integer, 
  string_value varchar(100),
  date_value varchar(30),
);
ALTER TABLE AttrVal_1000_PrtcpntObsrvtn
   ADD FOREIGN KEY (ontology_term_id) REFERENCES AttributeGraph (ontology_term_id);
CREATE INDEX AttrVal_1000_PrtcpntObsrvtn_i1
ON AttrVal_1000_PrtcpntObsrvtn (ontology_term_id, PrtcpntObsrvtn_id, number_value, string_value, date_value);
-- for test db only
CREATE unique INDEX AttrVal_1000_PrtcpntObsrvtn_i2 ON AttrVal_1000_PrtcpntObsrvtn (ontology_term_id, PrtcpntObsrvtn_id);

create table Ancestors_1000_PrtcpntObsrvtn (
  PrtcpntObsrvtn_id integer,
  Prtcpnt_id integer,
  Hshld_id integer,
  PRIMARY KEY (PrtcpntObsrvtn_id)
);



