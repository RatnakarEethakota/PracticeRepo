# Databricks notebook source
# DBTITLE 1,Mounting
dbutils.fs.mount(
    source='wasbs://raw@lakhsmistorage3.blob.core.windows.net',
    mount_point='/mnt/azuredeproject',
    extra_configs={'fs.azure.account.key.lakhsmistorage3.blob.core.windows.net':'ANxqNsu2j1oTIWYyee/x6ifkwKMRHmjtgqhkJnbVMyXR3nx50thBP+PBAkG2quIZrVsq/8QuE9xj+AStKtiUyg=='}
)

# COMMAND ----------

dbutils.fs.ls('/mnt/azuredeproject')

# COMMAND ----------

df.columns

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import split,col

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("pizza_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("pizza_name_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("order_time", TimestampType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_price", DoubleType(), True),
    StructField("pizza_size", StringType(), True),
    StructField("pizza_category", StringType(), True),
    StructField("pizza_ingredients", StringType(), True),
    StructField("pizza_name", StringType(), True)
])


# COMMAND ----------

# DBTITLE 1,Reading file
df=spark.read.format('csv').options(Header='True',delimeter=',').schema(schema).load('dbfs:/mnt/azuredeproject/dbo.pizza_sales.txt')

# COMMAND ----------

df1 = df.withColumn("quantity", split(col("quantity"),":")[0])
df1.display()

# COMMAND ----------

df1.createOrReplaceTempView('pizza_sales_analysis')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pizza_sales_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC order_id,
# MAGIC quantity,
# MAGIC order_date,
# MAGIC date_format(order_date,'MMM') month_name,
# MAGIC date_format(order_date,'EEEE') day_name,
# MAGIC hour(order_time) order_time,
# MAGIC unit_price,
# MAGIC total_price,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC
# MAGIC from pizza_sales_analysis

# COMMAND ----------

# DBTITLE 1,Pizza_Aggregates
# MAGIC %sql
# MAGIC select
# MAGIC count(distinct order_id) order_id,
# MAGIC sum(quantity) quantity,
# MAGIC order_date,
# MAGIC date_format(order_date,'MMM') month_name,
# MAGIC date_format(order_date,'EEEE') day_name,
# MAGIC hour(order_time) order_time,
# MAGIC sum(unit_price) unit_price,
# MAGIC sum(total_price) total_sales,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from pizza_sales_analysis
# MAGIC group by 3,4,5,6,9,10,11

# COMMAND ----------

