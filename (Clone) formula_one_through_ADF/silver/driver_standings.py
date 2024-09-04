# Databricks notebook source
results_df = spark.read.load('/mnt/adlsformula/f1racing/silver/results')
results_df = results_df.select('race_id','driver_id','constructor_id','points','position','position_text')

# COMMAND ----------

races_df = spark.read.load('/mnt/adlsformula/f1racing/silver/race')
races_df = races_df.select('race_id','season')

# COMMAND ----------

from pyspark.sql.functions import sum,when,col,count
results_races_df = results_df.join(races_df,'race_id','left')


# COMMAND ----------

total_points = results_races_df.groupBy('season','driver_id').agg(sum('points').alias('total_points'),count(when(col('position')==1,True)).alias('wins'))


# COMMAND ----------

# writing driver_standing table
# for bbc standing section those columns required for creation those only took for creation of driver_standing table

total_points.write.option('format','delta').mode('overwrite').save('/mnt/adlsformula/f1racing/gold/driver_standings')

# COMMAND ----------

from pyspark.sql.functions import desc
driver_standings_df = spark.read.load('/mnt/adlsformula/f1racing/gold/driver_standings')

driver_standings_df.display()

# COMMAND ----------

qualifying_df = spark.read.load('/mnt/adlsformula/f1racing/silver/qualifying')
constructor_df = spark.read.load('/mnt/adlsformula/f1racing/silver/constructors')
race_qualify_constructor_df = races_df.join(qualifying_df,'race_id','left')\
                                      .join(constructor_df,'constructor_id','left')
race_qualify_constructor_df = race_qualify_constructor_df.select('season',col('name').alias('Team'),'driver_id').distinct()
race_qualify_constructor_df.display()

# COMMAND ----------

driver_standings_df1 = driver_standings_df.join(race_qualify_constructor_df,['season','driver_id'],'left')
driver_standings_df1.display()

# COMMAND ----------

from pyspark.sql.functions import concat_ws
drivers_df = spark.read.load('/mnt/adlsformula/f1racing/silver/drivers')
drivers_df = drivers_df.select('driver_id',concat_ws(' ',col('forename'),col('surname')).alias('full name'))
drivers_df.display()


# COMMAND ----------

driver_standings_df2 = driver_standings_df1.join(drivers_df,'driver_id','left')
driver_standings_df2.display()

# COMMAND ----------



driver_standings_df2 = driver_standings_df2.select(col('full name').alias('Driver'),col('Team'),col('wins').alias('Wins'),col('total_points').alias('Points'),'season').orderBy(desc(col('Points')))
driver_standings_df2.filter(col('season')==2024).display()


# COMMAND ----------


