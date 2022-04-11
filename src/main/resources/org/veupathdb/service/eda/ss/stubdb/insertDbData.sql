

-- ============================================================
--    CHANGES TO THIS FILE WILL BREAK UNIT TESTS
-- ============================================================


------------------------------------------------------------
-- STUDY METADATA TABLES
------------------------------------------------------------

--(stable_id, display_nme, internal_abbrev)
insert into study values ('DS-2324', 'GEMS', 'ds2324');

insert into StudyIdDatasetId values('DS-2324', 'datasetid_2222');

--(stable_id, study_stable_id, parent_stable_id, internal_abbrev, display_name, display_name_plural, description, entity_type_id)
insert into entityTypeGraph values ('GEMS_House', 'DS-2324', null, 'Hshld', 'Household', 'Households', 'Households in the study',1,0,0);
insert into entityTypeGraph values ('GEMS_HouseObs', 'DS-2324', 'GEMS_House', 'HshldObsrvtn', 'Household Observation', 'Household Observations', 'Observations about households in the study',2,0,0);
insert into entityTypeGraph values ('GEMS_Part','DS-2324', 'GEMS_House', 'Prtcpnt', 'Participant', 'Participants', 'Participants in the study',3,0,0);
insert into entityTypeGraph values ('GEMS_PartObs', 'DS-2324', 'GEMS_Part', 'PrtcpntObsrvtn', 'Participant Observation', 'Participant Observations', 'Participant observations',4,0,0);
insert into entityTypeGraph values ('GEMS_Sample', 'DS-2324', 'GEMS_PartObs', 'Smpl', 'Sample', 'Samples', 'Sample',5,0,0);
insert into entityTypeGraph values ('GEMS_Treat', 'DS-2324', 'GEMS_PartObs', 'Trtmnt', 'Treatment', 'Treatments', 'Treatment',6,0,0);

------------------------------------------------------------
-- STUDY DATA TABLES
------------------------------------------------------------

-- households
-- stable_id, parent_stable_id, provider_label, display_name, definition, ordinal_values, display_type, display_order, display_range_min, display_range_max, range_min, range_max, bin_width_override, bin_width_computed, is_temporal, is_featured, is_merge_key, is_repeated, has_values, data_type, distinct_values_count, is_multi_valued, data_shape, unit, precision, impute_zero
insert into AttributeGraph_ds2324_Hshld values ('var_h1', null, '_address', 'City', 'The city where they live', '["Miami","Boston"]', 'default', 2, null, null, null, null, null, null, 0, 0, 0, 0, 1, 'string', null, 10, 0, 'categorical', null, null, 0);
insert into AttributeGraph_ds2324_Hshld values ('var_h2', null, '_roof', 'Roof', 'The roof type', null, 'default', 1, null, null, null, null, null, null, 0, 0, 0, 0, 1, 'string', null, 10, 0, 'categorical', null, null, 0);

insert into Ancestors_ds2324_Hshld values ('101');
insert into AttributeValue_ds2324_Hshld values ('101', 'var_h1', null, 'Miami', null);
insert into Ancestors_ds2324_Hshld values ('102');
insert into AttributeValue_ds2324_Hshld values ('102', 'var_h1', null, 'Boston', null);

-- household observations
-- stable_id, parent_stable_id, provider_label, display_name, definition, ordinal_values, display_type, display_order, display_range_min, display_range_max, range_min, range_max, bin_width_override, bin_width_computed, is_temporal, is_featured, is_merge_key, is_repeated, has_values, data_type, distinct_values_count, is_multi_valued, data_shape, unit, precision, impute_zero
insert into AttributeGraph_ds2324_HshldObsrvtn values ('var_ho1', null, '_watersupply', 'Water supply', 'Their water supply', '["piped","well"]', 'default', 1, null, null, null, null, null, null, 0, 0, 0, 0, 1, 'string', null, 10, 0, 'categorical', null, null, 0);

insert into Ancestors_ds2324_HshldObsrvtn values ('301', '101');
insert into AttributeValue_ds2324_HshldObsrvtn values ('301', 'var_ho1', null, 'piped', null);
insert into Ancestors_ds2324_HshldObsrvtn values ('302', '102');
insert into AttributeValue_ds2324_HshldObsrvtn values ('302', 'var_ho1', null, 'well', null);

-- participants
-- stable_id, parent_stable_id, provider_label, display_name, definition, vocabulary, display_type, display_order, display_range_min, display_range_max, range_min, range_max, bin_width_override, bin_width_computed, is_temporal, is_featured, is_merge_key, is_repeated, has_values, data_type, distinct_values_count, is_multi_valued, data_shape, unit, precision, impute_zero
insert into AttributeGraph_ds2324_Prtcpnt values ('var_p1', null, '_networth', 'Net worth', 'Their net worth', null, 'default', 1, null, null, '100', '100000', null, '50', 0, 0, 0, 0, 1, 'number', '["downloads", "variableTree"]', 10, 0, 'continuous', 'dollars', 2, 0);
insert into AttributeGraph_ds2324_Prtcpnt values ('var_p2', null, '_shoesize', 'Shoe size', 'Their shoe size', null, 'default', 1, null, null, '1.5', '14.5', null, '1', 0, 0, 0, 0, 1, 'number', null, 10, 0, 'categorical', 'size', 1, 0);
insert into AttributeGraph_ds2324_Prtcpnt values ('var_p3', null, '_name', 'Name', 'Their name', null, 'default', 1, null, null, null, null, null, null, 0, 0, 0, 0, 1, 'string', null, 10, 0, 'categorical', null, null, 0);
insert into AttributeGraph_ds2324_Prtcpnt values ('var_p4', null, '_haircolor', 'Hair color', 'Their hair color', '["blond","brown","silver"]', 'default', 1, null, null, null, null, null, null, 0, 0, 0, 0, 1, 'string', null, 10, 0, 'categorical', null, null, 0);
insert into AttributeGraph_ds2324_Prtcpnt values ('var_p5', null, '_earsize', 'Ear size', 'Their ear size', null, 'default', 1, null, null, null, null, null, null, 0, 0, 0, 0,1, 'string', null, 10, 0,  'categorical', null, null, 0);

insert into Ancestors_ds2324_Prtcpnt values ('201', '101');
insert into AttributeValue_ds2324_Prtcpnt values ('201', 'var_p3', null, 'Martin', null);
insert into AttributeValue_ds2324_Prtcpnt values ('201', 'var_p2', 11.5, null, null);
insert into AttributeValue_ds2324_Prtcpnt values ('201', 'var_p4', null, 'blond', null);
insert into AttributeValue_ds2324_Prtcpnt values ('201', 'var_p5', null, 'small', null);

insert into Ancestors_ds2324_Prtcpnt values ('202', '101');
insert into AttributeValue_ds2324_Prtcpnt values ('202', 'var_p3', null, 'Abe', null);
insert into AttributeValue_ds2324_Prtcpnt values ('202', 'var_p2', 10, null, null);
insert into AttributeValue_ds2324_Prtcpnt values ('202', 'var_p4', null, 'blond', null);
insert into AttributeValue_ds2324_Prtcpnt values ('202', 'var_p5', null, 'medium', null);

insert into Ancestors_ds2324_Prtcpnt values ('203', '102');
insert into AttributeValue_ds2324_Prtcpnt values ('203', 'var_p3', null, 'Gladys', null);
insert into AttributeValue_ds2324_Prtcpnt values ('203', 'var_p2', 11.5, null, null);
insert into AttributeValue_ds2324_Prtcpnt values ('203', 'var_p4', null, 'brown', null);
insert into AttributeValue_ds2324_Prtcpnt values ('203', 'var_p5', null, 'large', null);

insert into Ancestors_ds2324_Prtcpnt values ('204', '102');
insert into AttributeValue_ds2324_Prtcpnt values ('204', 'var_p3', null, 'Susan', null);
insert into AttributeValue_ds2324_Prtcpnt values ('204', 'var_p2', 10, null, null);
insert into AttributeValue_ds2324_Prtcpnt values ('204', 'var_p4', null, 'silver', null);
-- intentionally omit var_18 to test left join logic

-- participant observations
-- stable_id, parent_stable_id, provider_label, display_name, definition, vocabulary, display_type, display_order, display_range_min, display_range_max, range_min, range_max, bin_width_override, bin_width_computed, is_temporal, is_featured, is_merge_key, is_repeated, has_values, data_type, distinct_values_count, is_multi_valued, data_shape, unit, precision, impute_zero
insert into AttributeGraph_ds2324_PrtcpntObsrvtn values ('var_o1', null, '_weight', 'Weight', 'Their weight', null, 'default', 1, null, null, '0', '200', null, '5', 0, 0, 0, 0, 1, 'number', null, 10, 0, 'continuous', 'pounds', 2, 0);
insert into AttributeGraph_ds2324_PrtcpntObsrvtn values ('var_o2', null, '_favnumber', 'Favorite number', 'Their favorite number', null, 'default', 1, null, null, '0', '1000.123', null, '10', 0, 0, 0, 0, 1, 'number', null, 10, 0, 'categorical', null, null, 0);
insert into AttributeGraph_ds2324_PrtcpntObsrvtn values ('var_o3', null, '_startdate', 'Start date', 'Their start date', null, 'default', 1, null, null, to_date('1999','yyyy'), to_date('2010','yyyy'), null, 'week', 0, 0, 0, 0, 1, 'date', null, 10, 0, 'continuous', 'date', null, 0);
insert into AttributeGraph_ds2324_PrtcpntObsrvtn values ('var_o4', null, '_visitdate', 'Visit date', 'Their visit date', null, 'default', 1, null, null, to_date('1999','yyyy'), to_date('2010','yyyy'), 'month', 'week', 0, 0, 0, 0, 1, 'date', null, 10, 0, 'continuous', 'date', null, 0);
insert into AttributeGraph_ds2324_PrtcpntObsrvtn values ('var_o5', null, '_mood', 'Mood', 'Their mood', null, 'default', 1, null, null, null, null, null, null, 0, 0, 0, 0, 1, 'string', null, 10, 0, 'categorical', null, null, 0);

-- samples
insert into AttributeGraph_ds2324_Smpl values ('var_s1', null, '_density', 'Density', 'Their density', null, 'default', 1, null, null, null, null, null, null, 0, 0, 0, 0, 1, 'string', null, 10, 0, 'categorical', null, null, 0);

-- treatments
insert into AttributeGraph_ds2324_Trtmnt values ('var_t1', null, '_painkiller', 'Pain killer', 'Their pain killer', null, 'default', 1, null, null, null, null, null, null, 0, 0, 0, 0, 1, 'string', null, 10, 0, 'categorical', null, null, 0);


