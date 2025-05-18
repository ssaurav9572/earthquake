# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import StringType
# ensure the below library is installed on your fabric environment
import reverse_geocoder as rg
     

from datetime import date,timedelta
#start_date=date.today()-timedelta(7)
df = spark.read.table("earthquake_events_silver").filter(col('time') > start_date)


def get_country_code(lat, lon):
    """
    Retrieve the country code for a given latitude and longitude.

    Parameters:
    lat (float or str): Latitude of the location.
    lon (float or str): Longitude of the location.

    Returns:
    str: Country code of the location, retrieved using the reverse geocoding API.

    Example:
    >>> get_country_details(48.8588443, 2.2943506)
    'FR'
    """
    coordinates = (float(lat), float(lon))
    return rg.search(coordinates)[0].get('cc')

# registering the udfs so they can be used on spark dataframes
get_country_code_udf = udf(get_country_code, StringType())

# adding country_code and city attributes
df_with_location = \
                df.\
                    withColumn("country_code", get_country_code_udf(col("latitude"), col("longitude")))

# adding significance classification
df_with_location_sig_class = \
                            df_with_location.\
                                withColumn('sig_class', 
                                            when(col("sig") < 100, "Low").\
                                            when((col("sig") >= 100) & (col("sig") < 500), "Moderate").\
                                            otherwise("High")
                                            )
     



# appending the data to the gold table
df_with_location_sig_class.write.mode('append').saveAsTable('earthquake_events_gold')
