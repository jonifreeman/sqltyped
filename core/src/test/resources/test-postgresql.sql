CREATE SCHEMA sqltyped AUTHORIZATION sqltypedtest;
SET search_path TO sqltyped;
ALTER USER sqltypedtest SET search_path to sqltyped;

create table person(
  id bigserial NOT NULL,
  name varchar(255) NOT NULL, 
  age int NOT NULL, 
  salary int NOT NULL,
  img bytea,
  PRIMARY KEY (id)
);

create table job_history(
  person bigint references person(id) NOT NULL,
  name varchar(255) NOT NULL, 
  started timestamp NOT NULL, 
  resigned timestamp NULL
);

create table jobs(person varchar(255) NOT NULL,job varchar(255) NOT NULL);

insert into person values (1, 'joe', 36, 9500);
insert into person values (2, 'moe', 14, 8000);

insert into job_history values (1, 'Enron', '2002-08-02 08:00:00', '2004-06-22 18:00:00');
insert into job_history values (1, 'IBM', '2004-07-13 11:00:00', NULL);
insert into job_history values (2, 'IBM', '2005-08-10 11:00:00', NULL);
