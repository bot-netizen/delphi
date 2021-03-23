USE delphi_db;

DROP TABLE IF EXISTS job_instance_details ;
DROP TABLE IF EXISTS job_instance_status ;

create table job_instance_details(
    id INT(11) NOT NULL AUTO_INCREMENT,
    job_name VARCHAR(50),
    job_desc VARCHAR(200),
    job_priority INT(11),
    job_scheduler VARCHAR(20),
    job_frequency VARCHAR(20),
    cfg_name VARCHAR(100),
    json_string  TEXT,
    process_date DATE NOT NULL,
    intialized_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
CONSTRAINT job_instance_pk  PRIMARY KEY (id),
CONSTRAINT job_instance_uc UNIQUE (job_name , cfg_name, intialized_timestamp)
);

ALTER TABLE job_instance_details AUTO_INCREMENT = 1;

create table job_instance_status(
    instance_id INT(11) NOT NULL,
    application_id VARCHAR(50),
    status VARCHAR(20),
    modified_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
FOREIGN KEY (instance_id)
REFERENCES job_instance_details(id)
);