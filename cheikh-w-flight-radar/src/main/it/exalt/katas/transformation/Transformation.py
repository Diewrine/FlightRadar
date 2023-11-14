from pyspark.sql.functions import split, col, avg
from pyspark.sql.window import Window

import sys
sys.path.append('C:/Users/SMARPOINT/Desktop/Project/cheikh-w-flight-radar/cheikh-w-flight-radar/src/main/it/exalt/katas/common')
from CommonMethods import get_airlines, udf_retrieveContinent, EnrichData


def instruction_1(df, spark):
    df_1 = df.select("id", 'airline_icao', 'airline_name', 'status_text')
    df_1 = df_1.where(df_1.airline_icao != 'N/A') # REMOVE 'N/A' VALUES
    df_1 = df_1.withColumn('status_text', split(df_1['status_text'], " ").getItem(0))
    df_1 = df_1.where((df_1['status_text'] == 'Landed') | (df_1['status_text'] == 'Diverted'))

    df_1 = df_1.select("id", 'airline_icao', 'airline_name', 'status_text').groupBy("airline_icao").count()
    df_1 = df_1.withColumnRenamed('count', 'num_flight_per_airline')
    df_1 = df_1.sort(df_1.num_flight_per_airline.desc()) # ORDERING DATA
    max_airline_flight = df_1.limit(1)

    #get airlines data
    df_airlines = get_airlines(spark)
    result_1 = df_airlines.where(df_airlines.airline_icao == max_airline_flight.first()['airline_icao'])
    return EnrichData(result_1)

def instruction_2(df):
    df_2 = df.select('airline_name', 'status_text', 'origin_airport_country_name','destination_airport_country_name')
    df_2 = df_2.filter((df_2.origin_airport_country_name !="N/A") & (df_2.destination_airport_country_name !="N/A"))
    df_2 = df_2.withColumn('status_text', split(df_2['status_text'], " ").getItem(0))
    df_2 = df_2.where((df_2['status_text'] == 'Landed') | (df_2['status_text'] == 'Diverted'))
    #ADDING CONTINENT IN DATAFRAME
    df_2 = df_2.withColumn('origin_continent', udf_retrieveContinent(df_2.origin_airport_country_name)) \
        .withColumn('destination_continent', udf_retrieveContinent(df_2.destination_airport_country_name)) \
        .select('airline_name', 'status_text', 'origin_continent','destination_continent')
    df_2 = df_2.where(df_2['origin_continent'] == df_2['destination_continent'])
    df_2 = df_2.drop('origin_continent').withColumnRenamed('destination_continent', 'continent')
    #Remove 'unknow' continent
    df_2 = df_2.filter(df_2.continent != "Unknow")
    df_2 = df_2.groupBy("continent", "airline_name").count()

    windowSpecAgg  = Window.partitionBy("continent").orderBy(col("count").desc())
    df_2 = df_2.withColumn("max_vol", max(col("count")).over(windowSpecAgg))
    result_2 = df_2.where(df_2["count"] == df_2["max_vol"]).select("continent", "airline_name", "max_vol")
    return EnrichData(result_2)


def instruction_3(df):
    df_3 = df.select('id', 'aircraft_code', 'airline_name', 'registration', 'distance')
    df_3 = df_3.sort(df_3.distance.desc())
    result_3 = df_3.limit(1)
    return  EnrichData(result_3)


def instruction_4(df):
    df_4 = df.select('distance', 'origin_airport_country_name')
    df_4 = df_4.filter(df_4.origin_airport_country_name !="N/A")
    #ADDING CONTINENT IN DATAFRAME
    df_4 = df_4.withColumn('continent', udf_retrieveContinent(df_4.origin_airport_country_name)) \
        .select('distance', 'origin_airport_country_name','continent')
    result_4 = df_4.groupBy("continent") \
        .agg(avg("distance").alias("distance_moyenne"))
    return EnrichData(result_4)


def instruction_5(df):
    df_5 = df.select('id', 'aircraft_age', 'aircraft_code', 'status_text', 'aircraft_model')
    df_5 = df_5.withColumn('status_text', split(df_5['status_text'], " ").getItem(0))
    df_5 = df_5.where((df_5['status_text'] == 'Landed') | (df_5['status_text'] == 'Diverted'))
    df_5 = df_5.withColumn('aircraf_builder', split(df_5['aircraft_model'], " ").getItem(0))
    df_5 = df_5.groupBy('aircraf_builder').count()
    df_5 = df_5.withColumnRenamed('count', 'occurence_aircraft_builder')
    df_5 = df_5.sort(df_5.occurence_aircraft_builder.desc())
    result_5 = df_5.limit(1)
    return EnrichData(result_5)









