------------------------------------------------------------
-- STUDY METADATA TABLES
------------------------------------------------------------
insert into study values ("DS12385", "GEMS");

insert into entityName values (100, "Household", "Households", "House");
insert into entityName values (101, "Household Observation", "Household Observations", "HouseObs");
insert into entityName values (102, "Participant", "Participants", "Part");
insert into entityName values (103, "Participant Observation", "Participant Observations", "PartObs");
insert into entityName values (104, "Sample", "Samples", "Samp");
insert into entityName values (105, "Treatment", "Treatments", "Treat");

insert into entity values ("GEMS-House", 100, "DS12385", null, "Households from the study area");
insert into entity values ("GEMS-HouseObs", 101, "DS12385", "GEMS-House", "");
insert into entity values ("GEMS-Part", 102, "DS12385", "GEMS-House", "");
insert into entity values ("GEMS-PartObs", 103, "DS12385", "GEMS-Part", "");
insert into entity values ("GEMS-Sample", 104, "DS12385", "GEMS-PartObs", "");
insert into entity values ("GEMS-Treat", 105, "DS12385", "GEMS-PartObs", "");


------------------------------------------------------------
-- STUDY DATA TABLES
------------------------------------------------------------