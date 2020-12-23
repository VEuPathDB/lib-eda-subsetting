

-- ============================================================
--    CHANGES TO THIS FILE WILL BREAK UNIT TESTS
-- ============================================================


------------------------------------------------------------
-- STUDY METADATA TABLES
------------------------------------------------------------

--(stable_id, display_nme, internal_abbrev)
insert into study values ('DS-2324', 'GEMS', 'ds2324');

--(stable_id, study_stable_id, parent_stable_id, internal_abbrev, display_name, display_name_plural, description)
insert into entityTypeGraph values ('GEMS_House', 'DS-2324', null, 'Hshld', 'Household', 'Households', 'Households in the study');
insert into entityTypeGraph values ('GEMS_HouseObs', 'DS-2324', 'GEMS_House', 'HshldObsrvtn', 'Household Observation', 'Household Observations', 'Observations about households in the study');
insert into entityTypeGraph values ('GEMS_Part','DS-2324', 'GEMS_House', 'Prtcpnt', 'Participant', 'Participants', 'Participants in the study');
insert into entityTypeGraph values ('GEMS_PartObs', 'DS-2324', 'GEMS_Part', 'PrtcpntObsrvtn', 'Participant Observation', 'Participant Observations', 'Participant observations');
insert into entityTypeGraph values ('GEMS_Sample', 'DS-2324', 'GEMS_PartObs', 'Smpl', 'Sample', 'Samples', 'Sample');
insert into entityTypeGraph values ('GEMS_Treat', 'DS-2324', 'GEMS_PartObs', 'Trtmnt', 'Treatment', 'Treatments', 'Treatment');

------------------------------------------------------------
-- STUDY DATA TABLES
------------------------------------------------------------

-- households
insert into Attribute_ds2324_Hshld values ('var-18', 200, null, '_address', 'City', null, 1, 'string', 0, 'categorical', null, null, null);

insert into Ancestors_ds2324_Hshld values (101);
insert into AttrVal_ds2324_Hshld values (101, 'var-18', null, 'Miami', null);
insert into Ancestors_ds2324_Hshld values (102);
insert into AttrVal_ds2324_Hshld values (102, 'var-18', null, 'Boston', null);

-- household observations
insert into Attribute_ds2324_HshldObsrvtn values ('var-19', 200, null, '_watersupply', 'Water supply', null, 1, 'string', 0, 'categorical', null, null, null);

insert into Ancestors_ds2324_HshldObsrvtn values (301, 101);
insert into AttrVal_ds2324_HshldObsrvtn values (301, 'var-19', null, 'piped', null);
insert into Ancestors_ds2324_HshldObsrvtn values (302, 102);
insert into AttrVal_ds2324_HshldObsrvtn values (302, 'var-19', null, 'well', null);

-- participants

--(stable_id, ontology_term_id, parent_stable_id, provider_label, display_name, term_type, has_value, data_type, has_multiple_values_per_entity, data_shape, unit, unit_ontology_term_id, precision)
insert into Attribute_ds2324_Prtcpnt values ('var-10', 300, null, '_networth', 'Net worth', null, 1, 'number', 0, 'continuous', 'dollars', null, 2);
insert into Attribute_ds2324_Prtcpnt values ('var-11', 300, null, '_shoesize', 'Shoe size', null, 1, 'number', 0, 'categorical', 'size', null, 1);
insert into Attribute_ds2324_Prtcpnt values ('var-20', 200, null, '_name', 'Name', null, 1, 'string', 0, 'categorical', null, null, null);
insert into Attribute_ds2324_Prtcpnt values ('var-17', 200, null, '_haircolor', 'Hair color', null, 1, 'string', 0,  'categorical', null, null, null);

insert into Ancestors_ds2324_Prtcpnt values (201, 101);
insert into AttrVal_ds2324_Prtcpnt values (201, 'var-20', null, 'Martin', null);
insert into AttrVal_ds2324_Prtcpnt values (201, 'var-11', 11.5, null, null);
insert into AttrVal_ds2324_Prtcpnt values (201, 'var-17', null, 'blond', null);

insert into Ancestors_ds2324_Prtcpnt values (202, 101);
insert into AttrVal_ds2324_Prtcpnt values (202, 'var-20', null, 'Abe', null);
insert into AttrVal_ds2324_Prtcpnt values (202, 'var-11', 10, null, null);
insert into AttrVal_ds2324_Prtcpnt values (202, 'var-17', null, 'blond', null);

insert into Ancestors_ds2324_Prtcpnt values (203, 102);
insert into AttrVal_ds2324_Prtcpnt values (203, 'var-20', null, 'Gladys', null);
insert into AttrVal_ds2324_Prtcpnt values (203, 'var-11', 11.5, null, null);
insert into AttrVal_ds2324_Prtcpnt values (203, 'var-17', null, 'brown', null);

insert into Ancestors_ds2324_Prtcpnt values (204, 102);
insert into AttrVal_ds2324_Prtcpnt values (204, 'var-20', null, 'Susan', null);
insert into AttrVal_ds2324_Prtcpnt values (204, 'var-11', 10, null, null);
insert into AttrVal_ds2324_Prtcpnt values (204, 'var-17', null, 'silver', null);

-- participant observations

--(stable_id, ontology_term_id, parent_stable_id, provider_label, display_name, term_type, has_value, data_type, has_multiple_values_per_entity, data_shape, unit, unit_ontology_term_id, precision)
insert into Attribute_ds2324_PrtcpntObsrvtn values ('var-12', 300, null, '_weight', 'Weight', null, 1, 'number', 0, 'continuous', 'pounds', null, 2);
insert into Attribute_ds2324_PrtcpntObsrvtn values ('var-13', 300, null, '_favnumber', 'Favorite number', null,  1, 'number', 0, 'categorical', null, null, null);
insert into Attribute_ds2324_PrtcpntObsrvtn values ('var-14', 400, null, '_startdate', 'Start date', null,  1, 'date', 0, 'continuous', 'date', null, null);
insert into Attribute_ds2324_PrtcpntObsrvtn values ('var-15', 300, null, '_visitdate', 'Visit date', null,  1, 'date', 0, 'continuous', 'date', null, null);
insert into Attribute_ds2324_PrtcpntObsrvtn values ('var-16', 200, null, '_mood', 'Mood', null, 1, 'string', 0, 'categorical', null, null, null);



