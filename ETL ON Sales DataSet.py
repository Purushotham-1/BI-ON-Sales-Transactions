# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Project").getOrCreate()
print(spark)

# COMMAND ----------

df=spark.read.csv("/FileStore/tables/sales_data_sample-1.csv",header=True,inferSchema=True)
df.show(5)

# COMMAND ----------

df.show(5)
df.printSchema()
df.count()
df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import col,sum
df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Removing the ADDRESSLINE2 column because most of it is null values

# COMMAND ----------

df_1=df.drop("ADDRESSLINE2")
df_1.show(5)

# COMMAND ----------

missing_cols=[c for c in df_1.columns if df_1.filter(col(c).isNull()).count()>0]
print(missing_cols)

# COMMAND ----------

from pyspark.sql.functions import col, when


df_cleaned = df_1.dropna(subset=["ORDERNUMBER", "SALES", "CUSTOMERNAME", "PRODUCTCODE"])


df_cleaned = df_cleaned.fillna({"POSTALCODE": "UNKNOWN"})


df_cleaned.show(5)


# COMMAND ----------

df_cleaned.select("ORDERDATE").show(5)

# COMMAND ----------

from pyspark.sql.functions import to_date,col

df_transformed = df_cleaned.withColumn("ORDERDATE", to_date(col("ORDERDATE"), "M/d/yyyy H:mm")) \
                           .withColumn("YEAR_ID", col("YEAR_ID").cast("int")) \
                           .withColumn("QTR_ID", col("QTR_ID").cast("int")) \
                           .withColumn("MONTH_ID", col("MONTH_ID").cast("int")) \
                           .withColumn("SALES", col("SALES").cast("double")) \
                           .withColumn("PRICEEACH", col("PRICEEACH").cast("double"))

df_transformed.printSchema()
df_transformed.show(5)



# COMMAND ----------

from pyspark.sql.functions import year, month, expr

df_transformed = df_transformed.withColumn("ORDER_YEAR", year(col("ORDERDATE"))) \
                               .withColumn("ORDER_MONTH", month(col("ORDERDATE")))

df_transformed = df_transformed.withColumn(
    "SALES_CATEGORY",
    expr("CASE WHEN SALES >= 5000 THEN 'HIGH' WHEN SALES BETWEEN 1000 AND 4999 THEN 'MEDIUM' ELSE 'LOW' END")
)

df_transformed.select("ORDERDATE", "ORDER_YEAR", "ORDER_MONTH", "SALES", "SALES_CATEGORY").show(5)


# COMMAND ----------

sales_fact = df_transformed.select(
    "ORDERNUMBER", "CUSTOMERNAME", "PRODUCTCODE", "SALES", "QUANTITYORDERED", 
    "PRICEEACH", "ORDERDATE", "YEAR_ID", "MONTH_ID", "QTR_ID", "SALES_CATEGORY"
)
customer_dim = df_transformed.select("CUSTOMERNAME", "PHONE", "CITY", "STATE", "COUNTRY", "POSTALCODE").distinct()
product_dim = df_transformed.select("PRODUCTCODE", "PRODUCTLINE", "MSRP", "DEALSIZE").distinct()

sales_fact.show(5)
customer_dim.show(5)
product_dim.show(5)

# COMMAND ----------

# MAGIC %pip install snowflake-connector-python
# MAGIC

# COMMAND ----------

sfOptions = {
    "sfURL": "https://ab39765.us-east-2.aws.snowflakecomputing.com",
    "sfDatabase": "MY_DATABASE",
    "sfSchema": "MY_SCHEMA",
    "sfWarehouse": "COMPUTE_WH",
    "user": "PURUSHOTHAM",
    "password": "Chinni@1234567"
}




# COMMAND ----------

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Project").getOrCreate()

df = spark.read \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("query", "SELECT CURRENT_VERSION()") \
    .load()

df.show()


# COMMAND ----------

sales_fact.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "SALES_FACT") \
    .mode("overwrite") \
    .save()


# COMMAND ----------

customer_dim.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CUSTOMER_DIM") \
    .mode("overwrite") \
    .save()


# COMMAND ----------

product_dim.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "PRODUCT_DIM") \
    .mode("overwrite") \
    .save()

