# Databricks notebook source
# MAGIC %run /Workspace/formula_one_through_ADF/bronze/cmn_fun

# COMMAND ----------

pit_stop_df =spark.read.json('/mnt/adlsformula/f1racing/bronze/pitstop/',recursiveFileLookup=True)


# COMMAND ----------

from pyspark.sql.functions import col,explode,concat

# COMMAND ----------

pit_stop_df = pit_stop_df.withColumn('list',explode(col('MRData').RaceTable.Races))\
        .withColumn('lst',explode(col('list').PitStops))



# COMMAND ----------

pit_stops_df = pit_stop_df.withColumn('driver_id',col('lst').driverId)\
                            .withColumn('pit',col('lst').stop)\
                            .withColumn('lap',col('lst').lap)\
                            .withColumn('time',col('lst').time)\
                            .withColumn('miliseconds',col('lst').duration)
pit_stops_df =pit_stops_df.withColumn('season',col('MRData').RaceTable.season)\
                    .withColumn('round',col('MRData').RaceTable.round)\
                    .withColumn('race_id',concat('season','round'))

pit_stops_df = pit_stops_df.drop('season','round','MRData','list','lst')
pit_stops_df =pit_stops_df.select('race_id','driver_id','pit','lap','time','miliseconds')
pit_stops_df.display()
print(pit_stops_df.count())



# COMMAND ----------

pit_stops_df.columns


# COMMAND ----------

pit_stops_df1 = pit_stops_df.select('race_id','driver_id', 'pit', 'lap', 'time', 'miliseconds').distinct()
pit_stops_df1.count()

# COMMAND ----------

pit_stops_df1.write.option('format','delta').mode('overwrite').save('/mnt/adlsformula/f1racing/silver/pit_stops')

# COMMAND ----------

spark.read.load('/mnt/adlsformula/f1racing/silver/pit_stops')
