CREATE TABLE IF NOT EXISTS
bjj_lin.athlete
(id INT NOT NULL AUTO_INCREMENT,
athlete_name VARCHAR(200),
UNIQUE(athlete_name),
PRIMARY KEY(id));

CREATE TABLE IF NOT EXISTS
bjj_lin.academy
(id INT NOT NULL AUTO_INCREMENT,
academy_name VARCHAR(200),
UNIQUE(academy_name),
PRIMARY KEY(id));

CREATE TABLE IF NOT EXISTS
bjj_lin.athlete_academy
(athlete_id INT, 
 academy_id INT,
 FOREIGN KEY(athlete_id) REFERENCES bjj_lin.athlete(id),
 FOREIGN KEY(academy_id) REFERENCES bjj_lin.academy(id));
 
 CREATE TABLE IF NOT EXISTS
bjj_lin.event
(id INT NOT NULL AUTO_INCREMENT,
name VARCHAR(500) NOT NULL,
organization VARCHAR(200),
gi_or_no_gi VARCHAR(200),
rules_description VARCHAR(200),
year INT,
PRIMARY KEY (id),
UNIQUE(name, gi_or_no_gi, year));

CREATE TABLE IF NOT EXISTS
bjj_lin.division
(id INT NOT NULL AUTO_INCREMENT,
age_group VARCHAR(100) NOT NULL,
skill_rank VARCHAR(100) NOT NULL,
gender VARCHAR(100) NOT NULL,
weight_group VARCHAR(100) NOT NULL,
PRIMARY KEY (id),
UNIQUE(age_group, skill_rank, gender, weight_group));
 






