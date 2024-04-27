# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingestion of Races file

# COMMAND ----------

file_name = dbutils.widgets.text("file_name","")
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %run ../common_functions/cmn_functions

# COMMAND ----------

race_schema = StructType(fields=[
            StructField("raceId",IntegerType(),True),
            StructField("year",IntegerType(),True),
            StructField("round",IntegerType(),True),
            StructField("circuitId",IntegerType(),True),
            StructField("name",StringType(),True),
            StructField("date",DateType(),True),
            StructField("time",StringType(),True),
            StructField("url",StringType(),True)
           
])

# COMMAND ----------

races_df = spark.read.csv(f"/mnt/sagen2databricks/bronze/{file_name}.csv",header=True,schema=race_schema)
display(races_df)

# COMMAND ----------

racess_rename = races_df.withColumnRenamed("raceId","race_Id")\
                        .withColumnRenamed("circuitId","circuit_Id")\
                        .withColumnRenamed("year","race_year")

# COMMAND ----------

output_df = add_column(racess_rename)

# COMMAND ----------

output_df.write.mode("overwrite").parquet(f"/mnt/sagen2databricks/silver/{file_name}")
