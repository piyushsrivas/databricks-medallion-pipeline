# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_airports

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/flights/data/")


df = df.withColumn("flight_date",to_date(col("flight_date")))\
       .drop("_rescued_data")\
        .withColumn("modifiedDate",current_timestamp())

display(df)

# COMMAND ----------

import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.table(
    name = "stage_brooking"
)
def stage_brooking():
    
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df 

# COMMAND ----------

@dlt.view(
    name = "trans_bookings"
)
def trans_bookings():

    df = spark.readStream.table("stage_brooking")
    df = df.withColumn("amount",col("amount").cast(DoubleType()))\
           .withColumn("modifiedDate",current_timestamp())\
           .withColumn("booking_date",to_date(col("booking_date")))\
           .drop("rescued_data")

    return df
             


# COMMAND ----------

rules =  {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "silver_booking"
)
@dlt.expect_all_or_drop(rules)
def silver_booking():
    df = spark.readStream.table("trans_bookings")
    return df

    