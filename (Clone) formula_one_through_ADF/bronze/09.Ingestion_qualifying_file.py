# Databricks notebook source
# MAGIC %run /Workspace/formula_one_through_ADF/bronze/cmn_fun

# COMMAND ----------

qualifying_df = spark.read.json('/mnt/adlsformula/f1racing/bronze/qualifying/',recursiveFileLookup=True)
qualifying_df.display()

# COMMAND ----------

from pyspark.sql.functions import col,explode,concat

# COMMAND ----------

qualifying_df = qualifying_df.withColumn('list',explode(col('MRData').RaceTable.Races)).withColumn('lst',explode(col('list').QualifyingResults))
qualifying_df.display()

# COMMAND ----------

qualifying_df = qualifying_df.withColumn('constructor_id',col('lst').Constructor.constructorId)\
    .withColumn('driver_id',col('lst').Driver.driverId)\
    .withColumn('number',col('lst').number)\
    .withColumn('position',col('lst').position)\
    .withColumn('q1',col('lst').Q1)\
    .withColumn('q2',col('lst').Q2)\
    .withColumn('q3',col('lst').Q3)

qualifying_df =qualifying_df.withColumn('season',col('MRData').RaceTable.season)\
                    .withColumn('round',col('MRData').RaceTable.round)\
                    .withColumn('race_id',concat('season','round'))

qualifying_df = qualifying_df.drop('season','round','MRData','list','lst')
qualifying_df =qualifying_df.select('race_id','constructor_id','driver_id','number','position','q1','q2','q3')
qualifying_df.display()
print(qualifying_df.count())

# COMMAND ----------

qualifying_df.columns


# COMMAND ----------

qualifying_df1 = qualifying_df.select('race_id','constructor_id', 'driver_id', 'number', 'position', 'q1', 'q2', 'q3').distinct()
qualifying_df1.count()

# COMMAND ----------

qualifying_df1.write.option('format','delta').mode('overwrite').save('/mnt/adlsformula/f1racing/silver/qualifying')
