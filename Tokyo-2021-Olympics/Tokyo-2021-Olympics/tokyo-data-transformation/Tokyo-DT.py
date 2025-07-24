# Databricks notebook source
# DBTITLE 1,Accessing raw data
# Opening door and keying in the house (aka ADLS Gen2)
spark.conf.set("fs.azure.account.key.tokyo2021olympicsstrgacc.dfs.core.windows.net", "secret_key")

# Grabbing something inside the house
df_athletes = spark.read.csv("abfss://tokyo-olypmics-2021@tokyo2021olympicsstrgacc.dfs.core.windows.net/Raw-Data/athletes.csv", header=True)

df_coaches = spark.read.csv("abfss://tokyo-olypmics-2021@tokyo2021olympicsstrgacc.dfs.core.windows.net/Raw-Data/coaches.csv", header=True)

df_entriesgender = spark.read.csv("abfss://tokyo-olypmics-2021@tokyo2021olympicsstrgacc.dfs.core.windows.net/Raw-Data/entriesgender.csv", header=True)

df_medals = spark.read.options(header=True, inferSchema=True).csv("abfss://tokyo-olypmics-2021@tokyo2021olympicsstrgacc.dfs.core.windows.net/Raw-Data/medals.csv") #inferSchema checks the dataframe where the CSV or JSON files is being read and automatically determine the data types of each column in the resulting DataFrame (efficient way)

df_teams = spark.read.csv("abfss://tokyo-olypmics-2021@tokyo2021olympicsstrgacc.dfs.core.windows.net/Raw-Data/teams.csv", header=True)

display(df_teams)

# COMMAND ----------

# df_athletes.show()
# df_coaches.show()
# df_coaches.printSchema()
df_entriesgender.show()
df_entriesgender.printSchema()

# COMMAND ----------

# DBTITLE 1,Transformation 1
from pyspark.sql.functions import col

df_entriesgender_clean = df_entriesgender.withColumn("Female", col("Female").cast("int"))\
    .withColumn("Male", col("Male").cast("int"))\
    .withColumn("Total", col("Total").cast("int"))
df_entriesgender_clean.show()
df_entriesgender_clean.printSchema()

# COMMAND ----------

df_medals.show()
df_medals.printSchema()

# Medals dataframe is already cleansed because while being read, inferSchema checked the data and automatically determined the appropriate data types

# COMMAND ----------

display(df_teams)
df_teams.printSchema()

# COMMAND ----------

# DBTITLE 1,Analysis 1
# Find top countries with the highest number of gold medals
from pyspark.sql.functions import *

top_gold_medals_countries = df_medals.orderBy(desc("Gold")).select("TeamCountry","Gold") # this is like saying select * from df_medals order by Gold desc limit 10
display(top_gold_medals_countries)

# COMMAND ----------

# DBTITLE 1,Analysis 2
# Calculate the average number of entries by gender for each discipline
from pyspark.sql.functions import *

avg_entries_by_gender = df_entriesgender_clean.groupBy("Discipline") \
    .avg("Female","Male").withColumnRenamed("avg(Female)","Avg_Female").withColumnRenamed("avg(Male)","Avg_Male")
display(avg_entries_by_gender)
display(df_entriesgender_clean.groupBy("Discipline").count())

# COMMAND ----------

