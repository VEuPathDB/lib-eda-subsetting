

-- ============================================================
--    CHANGES TO THIS FILE WILL BREAK UNIT TESTS
-- ============================================================


------------------------------------------------------------
-- STUDY METADATA TABLES
------------------------------------------------------------
insert into study values (1000, 'GEMS');

/*
CREATE TABLE EntityTypeGraph (
  entity_type_stable_id varchar(50) not null,
  entity_type_name varchar(30) not null,
  study_id integer not null,
  parent_entity_type_stable_id varchar(30),
  description varchar(100),
  abbrev varchar(20),
  PRIMARY KEY (entity_type_stable_id),
);
*/

insert into entityTypeGraph values ('GEMS_House', 'Household', 1000, null, 'Households in the study', 'Hshld');
insert into entityTypeGraph values ('GEMS_HouseObs', 'Household Observation', 1000, 'GEMS_House', 'Observations about households in the study', 'HshldObsrvtn');
insert into entityTypeGraph values ('GEMS_Part', 'Participant', 1000, 'GEMS_House', 'Participants in the study', 'Prtcpnt');
insert into entityTypeGraph values ('GEMS_PartObs', 'Participant Observation', 1000, 'GEMS_Part', 'Participant observations', 'PrtcpntObsrvtn');
insert into entityTypeGraph values ('GEMS_Sample', 'Sample', 1000, 'GEMS_PartObs', 'Sample', 'Smpl');
insert into entityTypeGraph values ('GEMS_Treat', 'Treat', 1000, 'GEMS_PartObs', 'Treatment', 'Trtmnt');

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





