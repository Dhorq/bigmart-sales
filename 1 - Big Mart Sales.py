# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading JSON

# COMMAND ----------

df_json = spark.read.format('json').option('inferSchema', True)\
  .option('header', True)\
    .option('multiLine', False)\
      .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #DDL Schema

# COMMAND ----------

my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING,
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING,
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE

                '''

# COMMAND ----------

df = spark.read.format('csv')\
            .schema(my_ddl_schema)\
                .option('header', True)\
                    .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_strct_schema = StructType([
                                StructField('Item_Identifier', StringType(), True),
                                StructField('Item_Weight', StringType(), True),
                                StructField('Item_Fat_Content', StringType(), True),
                                StructField('Item_Visibility', StringType(), True),
                                StructField('Item_MRP', StringType(), True),
                                StructField('Outlet_Identifier', StringType(), True),
                                StructField('Outlet_Establishment_Year', StringType(), True),
                                StructField('Outlet_Size', StringType(), True),
                                StructField('Outlet_Location_Type', StringType(), True),
                                StructField('Outlet_Type', StringType(), True),
                                StructField('Item_Outlet_Sales', StringType(), True),
])

# COMMAND ----------

df = spark.read.format('csv')\
            .schema(my_strct_schema)\
                .option('header', True)\
                    .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # SELECT

# COMMAND ----------

df.display()

# COMMAND ----------

 df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # FILTER/WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 1

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 2

# COMMAND ----------

df.filter( (col('Outlet_Size').isNull() ) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Scenario - 1

# COMMAND ----------

df = df.withColumn('flag', lit("new"))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('multiply', col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Scenario - 2

# COMMAND ----------

df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Regular", "Reg"))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Low Fat", "Lf")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Type Casting

# COMMAND ----------

df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Sort / Order By

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Scenario - 1

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Scenario - 2

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Scenario - 3

# COMMAND ----------

df.sort(['Item_Weight', 'Item_Visibility'], ascending=[0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Scenario - 4

# COMMAND ----------

df.sort(['Item_Weight', 'Item_Visibility'], ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Drop

# COMMAND ----------

df.drop(col('Item_Visibility')).display()

# COMMAND ----------

df.drop('Item_Fat_Content', 'Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Duplicate

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Union & Union by Name

# COMMAND ----------

data1 = [('1', 'kad'),
         ('2', 'sid')]
schema1 = 'id STRING, name STRING'

df1 = spark.createDataFrame(data1, schema1)

data2 = [('3', 'rahul'),
         ('4', 'jas')]
schema2 = 'id STRING, name STRING'

df2 = spark.createDataFrame(data2, schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data3 = [('kad', '1'),
         ('sid', '2')]
schema3 = 'name STRING, id STRING'

df3 = spark.createDataFrame(data3, schema3)

# COMMAND ----------

df3.display()

# COMMAND ----------

df3.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Date Functions

# COMMAND ----------

df = df.withColumn('curr_date', current_date())

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Date_Add

# COMMAND ----------

df = df.withColumn('week_after', date_add('curr_date', 7))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Date Sub

# COMMAND ----------

df.withColumn('week_before', date_sub('curr_date', 7)).display()

# COMMAND ----------

df = df.withColumn('week_before', date_add('curr_date', -7))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### DATEDIFF

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
df = df.withColumn('datediff', datediff('week_after','curr_date'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Date Format

# COMMAND ----------

df = df.withColumn('week_before', date_format('week_before','dd-MM-yyyy'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Handling Nulls

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

df.fillna('Not Available').display()

# COMMAND ----------

df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType())) 
df.fillna('Not Available', subset=['Item_Weight']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Split and Index

# COMMAND ----------

df.withColumn('Outlet_Type', split('Outlet_Type', ' ')).display()

# COMMAND ----------

df.withColumn('Outlet_Type', split('Outlet_Type', ' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Explode

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type', split('Outlet_Type', ' '))

df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type', explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Array Contains

# COMMAND ----------

df_exp.withColumn('Type1_Flag',array_contains('Outlet_Type','Type1')).display()