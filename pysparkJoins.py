# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.types import *

# COMMAND ----------

data1=[(1,"Ajit",16000,2),
       (2,"Vishal",15000,1),
       (3,"Ram",20000,4)
      ]
schema1=["id","name","salary","dept"]

data2=[(3,"IT"),(2,"HR"),(1,"PAYROLL")]
schema2=["id","name"]

empDf=spark.createDataFrame(data=data1,schema=schema1)
deptDf=spark.createDataFrame(data=data2,schema=schema2)

print("empDf")
empDf.show()
print("deptDf")
deptDf.show()

# COMMAND ----------

#-------------------------------------------------------------------------------------------------------------
#          Join syntax
#          df1.join(df2,joinExpression,"typeOfJoin")
#-------------------------------------------------------------------------------------------------------------

#inner join
#syntax=df1.join(df2,joinExpression,"inner")
empDf.join(deptDf,empDf.dept==deptDf.id,"inner").show()

#left join
#syntax=df1.join(df2,joinExpression,"left")
empDf.join(deptDf,empDf.dept==deptDf.id,"left").show()

#right join
#syntax=df1.join(df2,joinExpression,"right")
empDf.join(deptDf,empDf.dept==deptDf.id,"right").show()

# COMMAND ----------

# left semi join
# syntax= df.join(df2,joinExpression,"leftsemi")
empDf.join(deptDf,empDf.dept==deptDf.id,"leftsemi").show()

#left anti join
#syntax=df.join(df2,joinExpression,"leftanti")
empDf.join(deptDf,empDf.dept==deptDf.id,"leftanti").show()




# COMMAND ----------

from pyspark.sql.functions import*
data3=[(1,"ram",0),(2,"Krishna",1),(3,"Hari",2)]
schema3=["id","name","mngid"]

df3=spark.createDataFrame(data=data3,schema=schema3).show()

#self join
#syntax=df.join(df,joinExpression,"inner")
df
df3.("empData").join(df3.alias("mngData"),col("empData.id")==col("mngData.mngid"),"inner").show()
