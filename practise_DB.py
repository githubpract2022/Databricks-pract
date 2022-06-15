# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

raw_location = "abfss://dataexpo-data@adlsreaddemoprerun.dfs.core.windows.net/rawdata/"

# COMMAND ----------

##second commit

##create sub branch 

# COMMAND ----------

read_1987=spark.read.option('header','true').csv(raw_location+'1987.csv')

# COMMAND ----------

read_1987.take(2)

# COMMAND ----------

df_ft=read_1987.where(read_1987.DayofMonth==14)

# COMMAND ----------

dir(df_ft)

# COMMAND ----------

sql_df=spark.sql('select ArrDelay from df_ft')

# COMMAND ----------

df_ft.createOrReplaceTempView('filter_csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from filter_csv

# COMMAND ----------

sql_df=spark.sql('select ArrDelay from filter_csv')

# COMMAND ----------

marina_df=sql_df.withColumnRenamed('ArrDelay','AreyEntraIDi')

# COMMAND ----------

display(marina_df)

# COMMAND ----------

[(name,type(getattr(marina_df,name))) for name in dir(marina_df)]

# COMMAND ----------

dir(spark.read.option)

# COMMAND ----------


