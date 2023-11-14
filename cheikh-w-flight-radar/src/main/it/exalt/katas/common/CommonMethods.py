from FlightRadar24 import FlightRadar24API
from pyspark.sql.functions import current_timestamp, substring, translate, col
from pyspark.sql.types import StructType, StructField, StringType
import awoc
from pyspark.sql.functions import udf

fr_api = FlightRadar24API()


#function to get brut flights data from API

#Load flight data
def load_flight_data():
    df_details_with_distance = [] # Cette liste vide va contenir toutes les informations nécessaires pour faire les analyses
    for flight in fr_api.get_flights():
        try :
            details = fr_api.get_flight_details(flight.id)
            flight.set_flight_details(details)

            code = flight.origin_airport_iata

            if(code!='N/A'):
                airport = fr_api.get_airport(code)

                df_details_with_distance.append((
                    flight.id,
                    flight.aircraft_age,
                    flight.aircraft_code,
                    flight.aircraft_country_id,
                    flight.aircraft_history,
                    flight.aircraft_model,
                    flight.get_distance_from(airport),
                    flight.time,
                    flight.time_details,
                    flight.airline_name,
                    flight.airline_short_name,
                    flight.airline_iata,
                    flight.airline_icao,
                    flight.registration,
                    flight.status_text,
                    flight.origin_airport_name,
                    flight.origin_airport_country_code,
                    flight.origin_airport_country_name,
                    flight.origin_airport_visible,
                    flight.destination_airport_name,
                    flight.destination_airport_country_code,
                    flight.destination_airport_country_name,
                    flight.destination_airport_visible
                )
                )

            else:
                df_details_with_distance.append((
                    flight.id,
                    flight.aircraft_age,
                    flight.aircraft_code,
                    flight.aircraft_country_id,
                    flight.aircraft_history,
                    flight.aircraft_model,
                    0.0000,
                    flight.time,
                    flight.time_details,
                    flight.airline_name,
                    flight.airline_short_name,
                    flight.airline_iata,
                    flight.airline_icao,
                    flight.registration,
                    flight.status_text,
                    flight.origin_airport_name,
                    flight.origin_airport_country_code,
                    flight.origin_airport_country_name,
                    flight.origin_airport_visible,
                    flight.destination_airport_name,
                    flight.destination_airport_country_code,
                    flight.destination_airport_country_name,
                    flight.destination_airport_visible
                )
                )

        except Exception as e:
            # Gestion des exceptions générales
            print("Une erreur s'est produite :", str(e))

    return df_details_with_distance


def get_airlines(spark):
    airlines = fr_api.get_airlines()
    airlines_list= []

    for el in airlines:
        airlines_list.append([el['Name'], el['Code'], el['ICAO']])
    airlines_rdd = spark.sparkContext.parallelize(airlines_list)
    df_airlines_schema = StructType([
        StructField("Name", StringType(), nullable=True),
        StructField("Code", StringType(), nullable=True),
        StructField("airline_icao", StringType(), nullable=True)
    ])
    df_airlines = spark.createDataFrame(airlines_rdd, df_airlines_schema)
    return df_airlines


#Load Optimize data
def load_optimize(hdfs_path: str, sparkSe):
    return sparkSe.read.parquet(hdfs_path + "/*")

#Load safe data
def load_safe(hdfs_path: str, sparkSe):
    return sparkSe.read.parquet(hdfs_path + "/*")


#Convert data to spark dataframe
def toSparkDF(df, df_schema, sparkSe):
    return sparkSe.createDataFrame(df, df_schema)

#Add partition column
def EnrichData(df):
    df = df.withColumn("current_time", current_timestamp())

    df = df.withColumn('tech_year', substring('current_time', 1,4)) \
        .withColumn('tech_month', substring('current_time', 1,7)) \
        .withColumn('tech_day', substring('current_time', 1,10)) \
        .withColumn('file_horodate', translate(col("current_time"), "[- :.]", ""))
    return df

#Retrieve horodate variable
def HorodateFile(df):
    enriched_df = EnrichData(df)
    return enriched_df.first()["file_horodate"]


#Get continent from country
def retrieveContinent(country:str):
    my_world = awoc.AWOC()
    result = ''
    try:
        result = my_world.get_country_data(country)["Continent Name"]
    except Exception as e:
        result = 'Unknow'
    return result

udf_retrieveContinent = udf(retrieveContinent, StringType())