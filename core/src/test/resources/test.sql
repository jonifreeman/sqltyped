create table person(
  id bigint(20) NOT NULL auto_increment,
  name varchar(255) NOT NULL, 
  age INT NOT NULL, 
  salary INT NOT NULL,
  img BLOB,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

create table job_history(
  person bigint(20) NOT NULL,
  name varchar(255) NOT NULL, 
  started timestamp NOT NULL, 
  resigned timestamp NULL,
  FOREIGN KEY person_id_fk (person)
        REFERENCES person (id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION
) ENGINE=InnoDB;

create table jobs(person varchar(255) NOT NULL,job varchar(255) NOT NULL) ENGINE=InnoDB;

create table alltypes(
  a TINYINT NOT NULL,
  b SMALLINT NOT NULL,
  c MEDIUMINT NOT NULL,
  d INT NOT NULL, 
  e BIGINT NOT NULL,
  f FLOAT(24) NOT NULL,
  g FLOAT(53) NOT NULL,
  h DOUBLE NOT NULL,
  i BIT(2) NOT NULL,
  j DATE NOT NULL,
  k TIME NOT NULL,
  l DATETIME NOT NULL,
  m TIMESTAMP NOT NULL,
  n YEAR NOT NULL,
  o CHAR(255) NOT NULL,
  p VARCHAR(255) NOT NULL,
  q TEXT NOT NULL,
  r ENUM('v1','v2') NOT NULL,
  s SET('v1','v2') NOT NULL,
  t DECIMAL NOT NULL
) ENGINE=InnoDB;

insert into person values (1, 'joe', 36, 9500, NULL);
insert into person values (2, 'moe', 14, 8000, NULL);

insert into job_history values (1, 'Enron', '2002-08-02 08:00:00', '2004-06-22 18:00:00');
insert into job_history values (1, 'IBM', '2004-07-13 11:00:00', NULL);
insert into job_history values (2, 'IBM', '2005-08-10 11:00:00', NULL);

insert into alltypes values(
  1,
  1,
  1,
  1, 
  1,
  1.0,
  1.0,
  1.0,
  1,
  '2012-10-10',
  '14:00:00',
  '2012-10-10',
  '2012-10-10',
  2012,
  'a',
  'a',
  'a',
  'v1',
  'v1',
  1.0
);
