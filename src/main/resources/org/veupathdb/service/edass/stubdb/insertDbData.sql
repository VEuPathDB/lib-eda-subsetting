

-- ============================================================
--    CHANGES TO THIS FILE WILL BREAK UNIT TESTS
-- ============================================================


------------------------------------------------------------
-- STUDY METADATA TABLES
------------------------------------------------------------
insert into study values ('DS12385', 'GEMS');

insert into entityName values (100, 'Household', 'Households', 'House');
insert into entityName values (101, 'Household Observation', 'Household Observations', 'HouseObs');
insert into entityName values (102, 'Participant', 'Participants', 'Part');
insert into entityName values (103, 'Participant Observation', 'Participant Observations', 'PartObs');
insert into entityName values (104, 'Sample', 'Samples', 'Samp');
insert into entityName values (105, 'Treatment', 'Treatments', 'Treat');

insert into entity values ('GEMS-House', 100, 'DS12385', null, 'Households from the study area');
insert into entity values ('GEMS-HouseObs', 101, 'DS12385', 'GEMS-House', '');
insert into entity values ('GEMS-Part', 102, 'DS12385', 'GEMS-House', 'Participants in the study');
insert into entity values ('GEMS-PartObs', 103, 'DS12385', 'GEMS-Part', '');
insert into entity values ('GEMS-Sample', 104, 'DS12385', 'GEMS-PartObs', '');
insert into entity values ('GEMS-Treat', 105, 'DS12385', 'GEMS-PartObs', '');

insert into variableType values (200, 'string');
insert into variableType values (300, 'number');
insert into variableType values (400, 'date');

--(variable_id, variable_type_id, entity_id, parent_entity_id, display_name, has_values, is_continuous, units, precision)
insert into variable values ('var-10', 300, 'GEMS-Part', null, '_networth', 'Net worth', 1, 1, 'dollars', 2);
insert into variable values ('var-18', 200, 'GEMS-House', null, '_roof', 'Roof', 1, 0, null, null);
insert into variable values ('var-11', 300, 'GEMS-Part', null, '_shoesize', 'Shoe size', 1, 0, 'shoe size', 1);
insert into variable values ('var-17', 200, 'GEMS-Part', null, '_haircolor', 'Hair color', 1, 0, null, null);
insert into variable values ('var-12', 300, 'GEMS-PartObs', null, '_weight', 'Weight', 1, 1, 'pounds', 2);
insert into variable values ('var-13', 300, 'GEMS-PartObs', null, '_favnumber', 'Favorite number', 1, 0, '', null);
insert into variable values ('var-14', 400, 'GEMS-PartObs', null, '_birthdate', 'Birth date', 1, 1, 'date', null);
insert into variable values ('var-15', 300, 'GEMS-PartObs', null, '_startdate', 'Start date', 1, 0, null, null);
insert into variable values ('var-16', 200, 'GEMS-PartObs', null, '_mood', 'Mood', 1, 0, null, null);
insert into variable values ('var-19', 200, 'GEMS-HouseObs', null, '_watersupply', 'Water supply', 1, 0, null, null);

------------------------------------------------------------
-- STUDY DATA TABLES
------------------------------------------------------------