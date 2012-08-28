create table person(
  id bigint(20) NOT NULL auto_increment,
  name varchar(255) NOT NULL, 
  age INT NOT NULL, 
  salary INT NOT NULL,
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

insert into person values (1, 'joe', 36, 9500);
insert into person values (2, 'moe', 14, 8000);

insert into job_history values (1, 'Enron', '2002-08-02 08:00:00', '2004-06-22 18:00:00');
insert into job_history values (1, 'IBM', '2004-07-13 11:00:00', NULL);
