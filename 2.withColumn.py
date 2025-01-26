# Databricks notebook source
df_csv=spark.read.format("csv")\
                 .option("inferSchema",True)\
                 .option("header",True)\
                 .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

df_csv.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###withColumnRenamed

# COMMAND ----------

df_csv.withColumnRenamed('Item_Weight','Item_wt').display()

# COMMAND ----------

#Adding new column
df_csv.withColumn('multiply',col('Item_Weight')*col('Item_MRP')).display()

#Modifying existing column
df_csv.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Regular','Reg'))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','LF'))\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Type casting

# COMMAND ----------

df=df_csv.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sort/orderBy

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario 1

# COMMAND ----------

df_csv.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2

# COMMAND ----------

df_csv.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario 3

# COMMAND ----------

# Sorting on multiple column
df_csv.sort(['Item_Weight','Item_Visibility'],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###limit

# COMMAND ----------

df_csv.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###drop

# COMMAND ----------

#drop single column
df_csv.drop('Item_Visibility').display()

#drop multiple column
df_csv.drop('Item_Identifier','Item_Weight').limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###dropDuplicates

# COMMAND ----------

df_csv.dropDuplicates().display()

# COMMAND ----------

df_csv.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

#distinct function also work as same as dropDuplicates
df_csv.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### union and unionByName

# COMMAND ----------

# MAGIC %md
# MAGIC ####Preparing dataframe

# COMMAND ----------

data1=[(1,'Ram'),
       (2,'Krishna')]

schema1='id STRING,name STRING'

df1=spark.createDataFrame(data1,schema1)
df1.show()

data2=[(3,'Hari'),
       (4,'Rahul')]

schema2='id STRING,name STRING'

df2=spark.createDataFrame(data2,schema2)
df2.show()


# COMMAND ----------


