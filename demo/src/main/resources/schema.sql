create table person(
  id bigint(20) NOT NULL auto_increment,
  name varchar(255) NOT NULL,
  secret varchar(255),
  interview bigint(20),
  PRIMARY KEY (id),
  UNIQUE (name)
) ENGINE=InnoDB;

create table interview(
  id bigint(20) NOT NULL auto_increment,
  held_by bigint(20) NOT NULL,
  rating DOUBLE,
  FOREIGN KEY held_by_fk (held_by)
        REFERENCES person (id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

alter table person 
  ADD FOREIGN KEY interview_fk (interview) 
  REFERENCES interview (id)
  ON DELETE CASCADE
  ON UPDATE NO ACTION;

create table comment(
  text TEXT,
  created TIMESTAMP NOT NULL,
  author bigint(20) NOT NULL,  
  interview bigint(20) NOT NULL,
  FOREIGN KEY author_fk (author)
        REFERENCES person (id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION,
  FOREIGN KEY interview_fk (interview)
        REFERENCES interview (id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION
) ENGINE=InnoDB;
