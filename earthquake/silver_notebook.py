from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType


from datetime import date,timedelta
#start_date=date.today()-timedelta(7)
df = spark.read.option("multiline", "true").json(f"Files/{start_date}_earthquake_data.json")



# Welcome to your new notebook
# Type here in the cell editor to add code!
# df now is a Spark DataFrame containing JSON data
# Reshape earthquake data by extracting and renaming key attributes for further analysis.
df = \
df.\
    select(
        'id',
        col('geometry.coordinates').getItem(0).alias('longitude'),
        col('geometry.coordinates').getItem(1).alias('latitude'),
        col('geometry.coordinates').getItem(2).alias('elevation'),
        col('properties.title').alias('title'),
        col('properties.place').alias('place_description'),
        col('properties.sig').alias('sig'),
        col('properties.mag').alias('mag'),
        col('properties.magType').alias('magType'),
        col('properties.time').alias('time'),
        col('properties.updated').alias('updated')
        )



# Convert 'time' and 'updated' columns from milliseconds to timestamp format for clearer datetime representation.
df = df.\
    withColumn('time', col('time')/1000).\
    withColumn('updated', col('updated')/1000).\
    withColumn('time', col('time').cast(TimestampType())).\
    withColumn('updated', col('updated').cast(TimestampType()))
     


# appending the data to the gold table
df.write.mode('append').saveAsTable('earthquake_events_silver')
