# Databricks notebook source
results_df = spark.read.parquet('/mnt/sagen2databricks/silver/results')
results_df = results_df.select('race_id','driver_id','constructor_id','points','position','position_text')

# COMMAND ----------

races_df = spark.read.parquet('/mnt/sagen2databricks/silver/races')
races_df = races_df.select('race_id','race_year')

# COMMAND ----------

from pyspark.sql.functions import sum,when,col,count
results_races_df = results_df.join(races_df,'race_id','left')
results_races_df.display()


# COMMAND ----------

total_points = results_races_df.groupBy('race_year','constructor_id').agg(sum('points').alias('total_points'),count(when(col('position')==1,True)).alias('wins'))
total_points.display()


# COMMAND ----------

constructors_stand_df = spark.read.parquet('/mnt/sagen2databricks/gold/constructors_standings')


# COMMAND ----------

constructors_df = spark.read.parquet('/mnt/sagen2databricks/silver/constructors').select('constructor_id',col('constructor_name').alias('Team'))
constructors_df.display()

# COMMAND ----------

from pyspark.sql.functions import desc

bbc_constructor_df = total_points.join(constructors_df,'constructor_id','inner')
bbc_constructor_df.filter(col('race_year')==2015).select('Team',col('wins').alias('Wins'),col('total_points').alias('Points')).orderBy(desc('Points')).display()


# COMMAND ----------

from pyspark.sql.functions import desc
constructors_stand_df = constructors_stand_df.join(constructors_df,'constructor_id','inner')
constructors_stand_df.filter(col('season')==2024).select('Team',col('wins').alias('Wins'),col('total_points').alias('Points')).orderBy(desc('Points')).display()

