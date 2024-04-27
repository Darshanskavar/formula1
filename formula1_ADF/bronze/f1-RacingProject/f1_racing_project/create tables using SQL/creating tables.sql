-- Databricks notebook source
create database if not exists hive_metastore.f1_project_db

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS hive_metastore.f1_project_db COMMENT 'This is f1_project_db schema';

-- COMMAND ----------

--Check the current catalog
SELECT current_catalog()

-- COMMAND ----------

-- Change the catalog to hive_metastore
USE CATALOG hive_metastore

-- COMMAND ----------

-- CHECK THE CURRENT SCHEMA
SELECT current_database();

-- COMMAND ----------

-- Choose the schema/database
USE SCHEMA f1_project_db

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create circuits.csv file table

-- COMMAND ----------

-- Create a table using a CSV file located at a specific location
CREATE TABLE if NOT EXISTS f1_project_db.circuits (
    circuitId INT,
    circuitRef STRING,
    name STRING,
    location STRING,
    country STRING,
    lat FLOAT,
    lng FLOAT,
    alt INT,
    url STRING
)using csv LOCATION '/mnt/sagen2databricks/bronze/circuits.csv'
options(header "true")

-- COMMAND ----------

drop table f1_project_db.circuits 

-- COMMAND ----------

select * from f1_project_db.circuits

-- COMMAND ----------

desc extended f1_project_db.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create races.csv file table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_project_db.races;
CREATE TABLE IF NOT EXISTS f1_project_db.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS (path "/mnt/sagen2databricks/bronze/races.csv", header true)

-- COMMAND ----------

select * from f1_project_db.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create constructor.json file table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_project_db.constructors;
CREATE TABLE IF NOT EXISTS f1_project_db.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/sagen2databricks/bronze/constructors.json")

-- COMMAND ----------

select * from f1_project_db.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create drivers.json file table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_project_db.drivers;
CREATE TABLE IF NOT EXISTS f1_project_db.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path "/mnt/sagen2databricks/bronze/drivers.json")

-- COMMAND ----------

select * from f1_project_db.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create results.json file table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_project_db.results;
CREATE TABLE IF NOT EXISTS f1_project_db.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/sagen2databricks/bronze/results.json")

-- COMMAND ----------

select * from f1_project_db.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create pit_stops.json file table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_project_db.pit_stops;
CREATE TABLE IF NOT EXISTS f1_project_db.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "/mnt/sagen2databricks/bronze/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_project_db.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create csv file folder to table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_project_db.lap_times;
CREATE TABLE IF NOT EXISTS f1_project_db.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/sagen2databricks/bronze/lap_times")

-- COMMAND ----------

select * from f1_project_db.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create json file folder to table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_project_db.qualifying;
CREATE TABLE IF NOT EXISTS f1_project_db.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "/mnt/sagen2databricks/bronze/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_project_db.qualifying
