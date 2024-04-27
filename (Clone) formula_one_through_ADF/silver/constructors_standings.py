# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

results_df = spark.read.load('/mnt/adlsformula/f1racing/silver/results')
results_df = results_df.select('race_id','driver_id','constructor_id','points','position','position_text')

# COMMAND ----------

display(results_df)

# COMMAND ----------

races_df = spark.read.load('/mnt/adlsformula/f1racing/silver/race')
races_df = races_df.select('race_id','season')

# COMMAND ----------

from pyspark.sql.functions import sum,when,col,count
results_races_df = results_df.join(races_df,'race_id','left')


# COMMAND ----------

total_points = results_races_df.groupBy('season','constructor_id').agg(sum('points').alias('total_points'),count(when(col('position')==1,True)).alias('wins'))


# COMMAND ----------

# writing constructors_standing table
# for bbc standing section those columns required for creation those only took for creation of driver_standing table

total_points.write.option('format','delta').mode('overwrite').save('/mnt/adlsformula/f1racing/gold/constructors_standings')

# COMMAND ----------

constructors_stand_df = spark.read.load('/mnt/adlsformula/f1racing/gold/constructors_standings')


# COMMAND ----------

constructors_df = spark.read.load('/mnt/adlsformula/f1racing/silver/constructors').select('constructor_id',col('name').alias('Team'))

# COMMAND ----------

from pyspark.sql.functions import desc
constructors_stand_df = constructors_stand_df.join(constructors_df,'constructor_id','inner')
constructors_stand_df.filter(col('season')==2024).select('Team',col('wins').alias('Wins'),col('total_points').alias('Points')).orderBy(desc('Points')).display()


# COMMAND ----------


