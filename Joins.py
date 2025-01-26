# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark=SparkSession.builder.appName("joins").getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#Define a schema for students
student_schema=["student_id","student_name"]

#Define a data for students
student_data=[(1,"Ram"),(2,"Krishna"),(3,"Hari"),(4,"Ajit"),(5,"Akshay")]

df_student=spark.createDataFrame(data=student_data,schema=student_schema)
df_student.show()


# COMMAND ----------

#Define a schema for courses
course_schema=["student_id","course_name"]

#Define a data for courses
course_data=[(1,"Math"),(2,"Science"),(3,"History")]

df_course=spark.createDataFrame(data=course_data,schema=course_schema)
df_course.show()

# COMMAND ----------

df_student.createOrReplaceTempView("student_table")

# COMMAND ----------

df_course.createOrReplaceTempView("course_table")

# COMMAND ----------

#cross join(cartesian product of table)
crossJoin=spark.sql('''
                    SELECT * FROM student_table
                    CROSS JOIN course_table
                    ''')
crossJoin.show() 
crossJoin.count()                   

# COMMAND ----------

#Inner join 
innerJoin=spark.sql('''
                    SELECT s.student_id,s.student_name,c.course_name FROM student_table s
                    JOIN course_table c
                    ON s.student_id=c.student_id
                    ''')
innerJoin.show()                    

# COMMAND ----------

#Left join
leftJoin=spark.sql('''
                   SELECT * FROM student_table s
                   LEFT JOIN course_table c
                   ON s.student_id=c.student_id
                   ''')
leftJoin.show()


# COMMAND ----------

#Right join 
rightJoin=spark.sql('''
                    SELECT * FROM student_table s
                    RIGHT JOIN course_table c 
                    ON s.student_id=c.student_id
                    ''')

rightJoin.show()                    

# COMMAND ----------

#Full join
fullJoin=spark.sql('''
                   SELECT * FROM student_table s
                   FULL JOIN course_table c
                   ON s.student_id=c.student_id
                   ''')

fullJoin.show()

# COMMAND ----------

from pyspark.sql.functions import *

#Self join
user_schema=["user_id","name","age","emergency_contact"]
user_data=[(1,"Nitish",34,11),
           (2,"Ankit",32,11),
           (3,"Neha",23,1),
           (4,"Radhika",34,3),
           (8,"Abhinav",31,11),
           (11,"Rahul",29,8)]
user_df=spark.createDataFrame(user_data,user_schema)
user_df.show()
user_df.createOrReplaceTempView("user_table")
#------------------------------------------------------------------------------------------------------------
# In SQL 
selfJoin=spark.sql('''
                   SELECT u1.name AS user_name,u2.name AS emergecy_person_name FROM user_table u1
                   JOIN user_table u2
                   ON u1.user_id=u2.emergency_contact
                   ''')

selfJoin.show()   
#-------------------------------------------------------------------------------------------------------------
# In PySpark
#joinExpression=col("u1.user_id")==col("u2.emergency_contact")
selfJoin_df=user_df.alias("u1").join(user_df.alias("u2"),col("u1.user_id")==col("u2.emergency_contact"),"INNER")
#selfJoin_df=user_data.alias("u1").join(user_df.alias("u2"),u1.col("user_id")==u2.col("emergency_contact"),"INNER"))
selfJoin_df.show()
result=selfJoin_df.select(col("u1.name").alias("user_name"),
                       col("u2.name").alias("emergency_contact_name")).show()

# COMMAND ----------


