CREATE TABLE ENTITIES (
  ID INTEGER NOT NULL,
  NAME VARCHAR(25),
  PARENT_ID INTEGER,
  PRIMARY KEY (ID)
);

INSERT INTO ENTITIES values (1, 'households', null);
INSERT INTO ENTITIES values (2, 'participants', 1);
INSERT INTO ENTITIES values (3, 'observations', 2);
