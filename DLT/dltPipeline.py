import dlt 
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql.functions import col, expr

###############################
#Booking Data
@dlt.table(
    name = "stage_brookings"
)
def stage_brookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df 

@dlt.view(
    name = "trans_brookings"
)
def trans_brookings():
    df = spark.readStream.table("stage_brookings")
    df = df.withColumn("amount",col("amount").cast(DoubleType()))\
           .withColumn("modifiedDate",current_timestamp())\
           .withColumn("booking_date",to_date(col("booking_date")))\
           .drop("rescued_data")
    return df

rules =  {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL"
 }

@dlt.table(
    name = "silver_booking"
)
@dlt.expect_all_or_drop(rules)
def silver_booking():
    df = spark.readStream.table("trans_brookings")
    return df

############################
#Flights Data 
@dlt.view(
    name = "trans_flights"
)
def trans_flights():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "trans_flights",
    keys = ["flight_id"],
    sequence_by = col("flight_id"),
    stored_as_scd_type=1
    )

###################
#Passengers Data

@dlt.view(
    name = "trans_customers"
 )
def trans_customers():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/passengers/data/")
    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trans_customers",
    keys = ["passenger_id"],
    sequence_by = col("passenger_id"),
    stored_as_scd_type = 1
)

########################
#Airports Data
@dlt.view(
    name = "trans_airports"
)
def trans_airports():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys = ["airport_id"],
    sequence_by = col("airport_id"),
    stored_as_scd_type = 1
)


@dlt.table(
    name = "silver_business"
)
def silver_business():
    booking = dlt.read("silver_booking").drop("_rescued_data")
    flights = dlt.readStream("silver_flights").drop("_rescued_data")
    passengers = dlt.readStream("silver_passengers").drop("_rescued_data")
    airports = dlt.readStream("silver_airports").drop("_rescued_data")
    df = booking\
        .join(flights,["flight_id"])\
        .join(passengers,["passenger_id"])\
        .join(airports,["airport_id"])
    return df
