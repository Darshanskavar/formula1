# Databricks notebook source
from pyspark.sql.functions import concat_ws,col
drivers = spark.read.load('/mnt/adlsformula/f1racing/silver/drivers')
drivers = drivers.select('driver_id',concat_ws(' ',col('forename'),col('surname')).alias('full name'),'number')

# COMMAND ----------

constructors = spark.read.load('/mnt/adlsformula/f1racing/silver/constructors')
constructors = constructors.select('constructor_id',col('name').alias('constructor_name'))

# COMMAND ----------

qualifying = spark.read.load('/mnt/adlsformula/f1racing/silver/qualifying')
qualifying = qualifying.select('race_id','driver_id','constructor_id','q1','q2','q3')
qualifying.display()

# COMMAND ----------

races = spark.read.load('/mnt/adlsformula/f1racing/silver/race')
races = races.select('race_id','season','date')

# COMMAND ----------

from pyspark.sql.functions import col
final_qualifying = drivers.join(qualifying,'driver_id','left')
final_qualifying = final_qualifying.join(constructors,'constructor_id','left')
final_qualifying = final_qualifying.join(races,'race_id','left')

final_qualifying = final_qualifying.select(col('full name').alias('Driver'),

                                           col('number').alias('Number'),
                                           col('constructor_name').alias('Team'),
                                           col('q1').alias('Qualifying1'),
                                           col('q2').alias('Qualifying2'),
                                           col('q3').alias('Qualifying3'),
                                           col('season'),col('date'))

final_qualifying = final_qualifying.filter(col('season')=='2024').orderBy(col('Qualifying1'))

                                           

# COMMAND ----------

final_qualifying.write.option('format','delta').mode('overwrite').save('/mnt/adlsformula/f1racing/gold/qualifying')
