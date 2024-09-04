# Databricks notebook source
dbutils.notebook.run("01.ingestion_circuite_file",0,{"file_name":"circuits"})

# COMMAND ----------

dbutils.notebook.run("02.Ingestion_races_file",0,{"file_name":"races"})

# COMMAND ----------

dbutils.notebook.run("03.Ingstion_constructor_file",0,{"file_name":"constructors"})

# COMMAND ----------

dbutils.notebook.run("04.Ingstion_drivers.json_file",0,{"file_name":"drivers"})

# COMMAND ----------

dbutils.notebook.run("05.Ingestion_results_file",0,{"file_name":"results"})

# COMMAND ----------

dbutils.notebook.run("06.Ingestion_pit_stopes_file",0,{"file_name":"pit_stops"})

# COMMAND ----------

dbutils.notebook.run("07.Ingestion_lap_times_folder",0,{"file_name":"lap_times"})

# COMMAND ----------

dbutils.notebook.run("08.Ingest_qualifying_folder",0,{"file_name":"qualifying"})
