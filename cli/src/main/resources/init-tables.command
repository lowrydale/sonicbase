start cluster
create database db
create table Employers (id VARCHAR(64), name VARCHAR(256))
create index employerId on Employers(id)
create table Persons (id1 BIGINT, id2 BIGINT, name VARCHAR(256), socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id1))
create table Memberships (personId BIGINT, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, personId2))
create table Resorts (resortId BIGINT, resortName VARCHAR(20), PRIMARY KEY (resortId))
create table Strings (id1 VARCHAR, id2 BIGINT, PRIMARY KEY (id1))
