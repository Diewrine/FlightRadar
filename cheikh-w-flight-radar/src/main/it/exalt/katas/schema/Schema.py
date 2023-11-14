from pyspark.sql.types import StructType, StructField, StringType

raw_schema = StructType([
    StructField('id', StringType(), True),
    StructField('aircraft_age', StringType(), True),
    StructField('aircraft_code', StringType(), True),
    StructField('aircraft_country_id', StringType(), True),
    StructField('aircraft_history', StringType(), True),
    StructField('aircraft_model', StringType(), True),
    StructField('distance', StringType(), True),
    StructField('time', StringType(), True),
    StructField('time_details', StringType(), True),
    StructField('airline_name', StringType(), True),
    StructField('airline_short_name', StringType(), True),
    StructField('airline_iata', StringType(), True),
    StructField('airline_icao', StringType(), True),
    StructField('registration', StringType(), True),
    StructField('status_text', StringType(), True),
    StructField('origin_airport_name', StringType(), True),
    StructField('origin_airport_country_code', StringType(), True),
    StructField('origin_airport_country_name', StringType(), True),
    StructField('origin_airport_visible', StringType(), True),
    StructField('destination_airport_name', StringType(), True),
    StructField('destination_airport_country_code', StringType(), True),
    StructField('destination_airport_country_name', StringType(), True),
    StructField('destination_airport_visible', StringType(), True)
])