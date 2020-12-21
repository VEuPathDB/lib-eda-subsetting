-- the EDA service doesn't need to know much about the Study, because the WDK will serve that data
-- the abbrev would be used in the name of the tall and ancestors tables
-- the study_id is a stable ID
create table Study (
--study_id  integer,                  -- JB not needed
  stable_id varchar(50) not null,     -- JB previously source_id
  name varchar(30) not null,
  PRIMARY KEY (stable_id)
);

-- The entities types per study, eg Participants, and their tree relationships
CREATE TABLE EntityTypeGraph (
--entity_type_graph_id integer,               -- JB not needed
  stable_id varchar(50) not null,             -- JB previously entity_type_stable_id
  display_name varchar(30) not null,          -- JB previously entity_type_name
  display_name_plural varchar(30) not null,   -- JB need a plural column
  study_stable_id varchar(50) not null,       -- JB previously study_id
  parent_stable_id varchar(30),               -- JB previously parent_entity_type_stable_id
  description varchar(100),
  abbrev varchar(20),
  PRIMARY KEY (entity_type_stable_id),
);
alter table EntityTypeGraph add unique (name, study_stable_id);
alter table EntityTypeGraph add unique (parent_stable_id, study_stable_id);
alter table EntityTypeGraph add unique (stable_id, study_stable_id);
ALTER TABLE EntityTypeGraph 
   ADD FOREIGN KEY (study_stable_id) REFERENCES Study (study_stable_id);

-- The variables/categories in a study, as a tree.  (It is called a graph because in non-EDA applications
-- attributes might have multiple parents.)
create table AttributeGraph (
--attribute_graph_id               -- JB not needed
  stable_id varchar(30),           -- JB previously 'source_id'
  study_stable_id varchar(30) not null,  -- JB previously study_id
  ontology_term_id integer,
  parent_stable_id varchar(30),      -- JB previously 'parent_source_id'
  parent_ontology_term_id integer,   -- JB why do we need this?
  provider_label varchar(30) not null,
  display_name varchar(30) not null,
  term_type varchar(20),
  PRIMARY KEY (stable_id)
);
alter table AttributeGraph add unique (ontology_term_id);
ALTER TABLE AttributeGraph
   ADD FOREIGN KEY (study_stable_id) REFERENCES Study (study_stable_id);
ALTER TABLE AttributeGraph 
   ADD FOREIGN KEY (parent_stable_id) REFERENCES AttributeGraph (stable_id);

-- the attributes that have values (ie, are not strictly a category).
-- rows here link to the AttributeGraph table for positioning in the tree.
-- we allow categories to have values, so items in this table might
-- link to rows in AttributeGraph that are parents there.
create table Attribute (
--attribute_id integer              -- JB not needed
  stable_id varchar(30),            -- JB previously 'source_id varchar(255)'  (why so large?)
  entity_type_stable_id varchar(30),
  process_type_id integer,
  ontology_term_id integer,         -- JB this seems redundant with the same col in AttributeGraph (a join away)
  data_type varchar(10),
  has_multiple_values_per_entity integer,
  data_shape varchar(10),
  unit varchar (30),
  unit_ontology_term_id integer,
  precision integer,
  PRIMARY KEY (stable_id)
);
ALTER TABLE Attribute 
   ADD FOREIGN KEY (entity_type_stable_id) REFERENCES EntityTypeGraph (entity_type_stable_id);

--------------------------------------------------------------------------------------------------
-- the following tables are per-study.  Their name is formed using the study ID, in this case 1000.
-- Each entity type gets two tables:
--    AttrVal_XXXXX_YYYYY
--    Ancestors_XXXXX_YYYYY
--  where XXXXX is the study ID and YYYYY is the entity's abbreviation
--------------------------------------------------------------------------------------------------

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



