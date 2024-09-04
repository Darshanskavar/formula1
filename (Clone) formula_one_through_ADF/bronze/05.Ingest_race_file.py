# Databricks notebook source
# MAGIC %run /Workspace/formula_one_through_ADF/bronze/cmn_fun

# COMMAND ----------

from pyspark.sql.functions import col,explode,concat


# COMMAND ----------

race_df = spark.read.option('multiline',True).json('/mnt/adlsformula/f1racing/bronze/races/', recursiveFileLookup=True)
race_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### we have to create 'race_id' column from 'season' and 'round'
# MAGIC for the race , result , lap_times and pit_stops files same procedure follow

# COMMAND ----------

race_df = race_df.withColumn('list',explode(col('MRData').RaceTable.Races))
race_df = race_df.drop('MRData')
race_df = race_df.withColumn('season',col('list').season).withColumn('round',col('list').round).withColumn('date',col('list').date)\
    .withColumn('race_name',col('list').raceName).withColumn('circuit_id',col('list').Circuit.circuitId)\
        .withColumn('race_id',concat('season','round'))
race_df = race_df.select('race_id','season', 'round', 'date', 'race_name', 'circuit_id')
race_df = race_df.drop('list')
race_df.display()
print(race_df.count())

# COMMAND ----------

race_df.columns

# COMMAND ----------

race_df = race_df.select('race_id','season', 'round', 'date', 'race_name', 'circuit_id').distinct()
race_df.count()

# COMMAND ----------

race_df.write.option('format','delta').mode('overwrite').save('/mnt/adlsformula/f1racing/silver/race')
