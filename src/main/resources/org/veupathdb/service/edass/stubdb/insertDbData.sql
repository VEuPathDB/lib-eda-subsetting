

-- ============================================================
--    CHANGES TO THIS FILE WILL BREAK UNIT TESTS
-- ============================================================


------------------------------------------------------------
-- STUDY METADATA TABLES
------------------------------------------------------------
insert into study values (1000, 'GEMS');

--(stable_id, entity_type_name, study_id, parent_stable_id, description, abbrev)
insert into entityTypeGraph values ('GEMS_House', 'Household', 1000, null, 'Households in the study', 'Hshld');
insert into entityTypeGraph values ('GEMS_HouseObs', 'Household Observation', 1000, 'GEMS_House', 'Observations about households in the study', 'HshldObsrvtn');
insert into entityTypeGraph values ('GEMS_Part', 'Participant', 1000, 'GEMS_House', 'Participants in the study', 'Prtcpnt');
insert into entityTypeGraph values ('GEMS_PartObs', 'Participant Observation', 1000, 'GEMS_Part', 'Participant observations', 'PrtcpntObsrvtn');
insert into entityTypeGraph values ('GEMS_Sample', 'Sample', 1000, 'GEMS_PartObs', 'Sample', 'Smpl');
insert into entityTypeGraph values ('GEMS_Treat', 'Treat', 1000, 'GEMS_PartObs', 'Treatment', 'Trtmnt');

--(stable_id, ontology_term_id, study_id, parent_stable_id, provider_label, display_name, term_type)
insert into AttributeGraph values ('var-10', 300, 1000, null, '_networth', 'Net worth', null);
insert into AttributeGraph values ('var-18', 200, 1000, null, '_address', 'City', null);
insert into AttributeGraph values ('var-11', 300, 1000, null, '_shoesize', 'Shoe size', null);
insert into AttributeGraph values ('var-20', 200, 1000, null, '_name', 'Name', null);
insert into AttributeGraph values ('var-17', 200, 1000, null, '_haircolor', 'Hair color', null);
insert into AttributeGraph values ('var-12', 300, 1000, null, '_weight', 'Weight', null);
insert into AttributeGraph values ('var-13', 300, 1000, null, '_favnumber', 'Favorite number', null);
insert into AttributeGraph values ('var-14', 400, 1000, null, '_startdate', 'Start date', null);
insert into AttributeGraph values ('var-15', 300, 1000, null, '_visitdate', 'Visit date', null);
insert into AttributeGraph values ('var-16', 200, 1000, null, '_mood', 'Mood', null);
insert into AttributeGraph values ('var-19', 200, 1000, null, '_watersupply', 'Water supply', null);

--(stable_id, ontology_term_id, entity_type_stable_id, has_multiple_values_per_entity, data_shape, unit, unit_ontology_term_id, precision)
insert into Attribute values ('var-10', 300, 'GEMS_Part', null, '_networth', 'Net worth', 1, 1, 'dollars', 2);
insert into Attribute values ('var-18', 200, 'GEMS_House', null, '_address', 'City', 1, 0, null, null);
insert into Attribute values ('var-11', 300, 'GEMS_Part', null, '_shoesize', 'Shoe size', 1, 0, 'shoe size', 1);
insert into Attribute values ('var-20', 200, 'GEMS_Part', null, '_name', 'Name', 1, 0, null, null);
insert into Attribute values ('var-17', 200, 'GEMS_Part', null, '_haircolor', 'Hair color', 1, 0, null, null);
insert into Attribute values ('var-12', 300, 'GEMS_PartObs', null, '_weight', 'Weight', 1, 1, 'pounds', 2);
insert into Attribute values ('var-13', 300, 'GEMS_PartObs', null, '_favnumber', 'Favorite number', 1, 0, '', null);
insert into Attribute values ('var-14', 400, 'GEMS_PartObs', null, '_startdate', 'Start date', 1, 1, 'date', null);
insert into Attribute values ('var-15', 300, 'GEMS_PartObs', null, '_visitdate', 'Visit date', 1, 0, null, null);
insert into Attribute values ('var-16', 200, 'GEMS_PartObs', null, '_mood', 'Mood', 1, 0, null, null);
insert into Attribute values ('var-19', 200, 'GEMS_HouseObs', null, '_watersupply', 'Water supply', 1, 0, null, null);

------------------------------------------------------------
-- STUDY DATA TABLES
------------------------------------------------------------
create table AttrVal_1000_Hshld (
  hshld_id integer,
  ontology_term_id integer,
  number_value integer,
  string_value varchar(100),
  date_value varchar(30),
);
-- households
insert into Ancestors_1000_Hshld values (101);
insert into AttrVal_1000_Hshld values (101, 'var-18', null, 'Miami', null);
insert into Ancestors_1000_Hshld values (102);
insert into AttrVal_1000_Hshld values (102, 'var-18', null, 'Boston', null);

-- household observations
insert into Ancestors_1000_HshldObsrvtn values (301, 101);
insert into AttrVal_1000_HshldObsrvtn values (301, 'var-19', null, 'piped', null);
insert into Ancestors_1000_HshldObsrvtn values (302, 102);
insert into AttrVal_1000_HshldObsrvtn values (302, 'var-19', null, 'well', null);

-- participants
insert into Ancestors_1000_Prtcpnt values (201, 101);
insert into AttrVal_1000_Prtcpnt values (201, 'var-20', null, 'Martin', null);
insert into AttrVal_1000_Prtcpnt values (201, 'var-11', 11.5, null, null);
insert into AttrVal_1000_Prtcpnt values (201, 'var-17', null, 'blond', null);

insert into Ancestors_1000_Prtcpnt values (202, 101);
insert into AttrVal_1000_Prtcpnt values (202, 'var-20', null, 'Abe', null);
insert into AttrVal_1000_Prtcpnt values (202, 'var-11', 10, null, null);
insert into AttrVal_1000_Prtcpnt values (202, 'var-17', null, 'blond', null);

insert into Ancestors_1000_Prtcpnt values (203, 102);
insert into AttrVal_1000_Prtcpnt values (203, 'var-20', null, 'Gladys', null);
insert into AttrVal_1000_Prtcpnt values (203, 'var-11', 11.5, null, null);
insert into AttrVal_1000_Prtcpnt values (203, 'var-17', null, 'brown', null);

insert into Ancestors_1000_Prtcpnt values (204, 102);
insert into AttrVal_1000_Prtcpnt values (204, 'var-20', null, 'Susan', null);
insert into AttrVal_1000_Prtcpnt values (204, 'var-11', 10, null, null);
insert into AttrVal_1000_Prtcpnt values (204, 'var-17', null, 'silver', null);





