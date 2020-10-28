

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

insert into entity values ('GEMS_House', 100, 'DS12385', null, 'Households from the study area');
insert into entity values ('GEMS_HouseObs', 101, 'DS12385', 'GEMS_House', '');
insert into entity values ('GEMS_Part', 102, 'DS12385', 'GEMS_House', 'Participants in the study');
insert into entity values ('GEMS_PartObs', 103, 'DS12385', 'GEMS_Part', '');
insert into entity values ('GEMS_Sample', 104, 'DS12385', 'GEMS_PartObs', '');
insert into entity values ('GEMS_Treat', 105, 'DS12385', 'GEMS_PartObs', '');

insert into variableType values (200, 'string');
insert into variableType values (300, 'number');
insert into variableType values (400, 'date');

--(variable_id, variable_type_id, entity_id, parent_entity_id, display_name, has_values, is_continuous, units, precision)
insert into variable values ('var-10', 300, 'GEMS_Part', null, '_networth', 'Net worth', 1, 1, 'dollars', 2);
insert into variable values ('var-18', 200, 'GEMS_House', null, '_address', 'City', 1, 0, null, null);
insert into variable values ('var-11', 300, 'GEMS_Part', null, '_shoesize', 'Shoe size', 1, 0, 'shoe size', 1);
insert into variable values ('var-20', 200, 'GEMS_Part', null, '_name', 'Name', 1, 0, null, null);
insert into variable values ('var-17', 200, 'GEMS_Part', null, '_haircolor', 'Hair color', 1, 0, null, null);
insert into variable values ('var-12', 300, 'GEMS_PartObs', null, '_weight', 'Weight', 1, 1, 'pounds', 2);
insert into variable values ('var-13', 300, 'GEMS_PartObs', null, '_favnumber', 'Favorite number', 1, 0, '', null);
insert into variable values ('var-14', 400, 'GEMS_PartObs', null, '_startdate', 'Start date', 1, 1, 'date', null);
insert into variable values ('var-15', 300, 'GEMS_PartObs', null, '_visitdate', 'Visit date', 1, 0, null, null);
insert into variable values ('var-16', 200, 'GEMS_PartObs', null, '_mood', 'Mood', 1, 0, null, null);
insert into variable values ('var-19', 200, 'GEMS_HouseObs', null, '_watersupply', 'Water supply', 1, 0, null, null);

------------------------------------------------------------
-- STUDY DATA TABLES
------------------------------------------------------------

-- households
insert into gems_house_ancestors values (101);
insert into gems_house_tall values (101, 'var-18', null, 'Miami', null);
insert into gems_house_ancestors values (102);
insert into gems_house_tall values (102, 'var-18', null, 'Boston', null);

-- household observations
insert into gems_houseobs_ancestors values (301, 101);
insert into gems_houseobs_tall values (301, 'var-19', null, 'piped', null);
insert into gems_houseobs_ancestors values (302, 102);
insert into gems_houseobs_tall values (302, 'var-19', null, 'well', null);

-- participants
insert into gems_part_ancestors values (201, 101);
insert into gems_part_tall values (201, 'var-20', null, 'Martin', null);
insert into gems_part_tall values (201, 'var-11', null, '11.5', null);
insert into gems_part_tall values (201, 'var-17', null, 'blond', null);

insert into gems_part_ancestors values (202, 101);
insert into gems_part_tall values (202, 'var-20', null, 'Abe', null);
insert into gems_part_tall values (202, 'var-11', null, '10', null);
insert into gems_part_tall values (202, 'var-17', null, 'blond', null);

insert into gems_part_ancestors values (203, 102);
insert into gems_part_tall values (203, 'var-20', null, 'Gladys', null);
insert into gems_part_tall values (203, 'var-11', null, '11.5', null);
insert into gems_part_tall values (203, 'var-17', null, 'brown', null);

insert into gems_part_ancestors values (204, 102);
insert into gems_part_tall values (204, 'var-20', null, 'Susan', null);
insert into gems_part_tall values (204, 'var-11', null, '10', null);
insert into gems_part_tall values (204, 'var-17', null, 'silver', null);





