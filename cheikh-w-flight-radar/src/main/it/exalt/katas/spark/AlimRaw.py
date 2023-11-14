import sys
from pyspark.sql.connect.session import SparkSession

sys.path.append('C:/Users/SMARPOINT/Desktop/Project/cheikh-w-flight-radar/cheikh-w-flight-radar/src/main/it\exalt/katas/common')
from CommonMethods import load_flight_data, toSparkDF
sys.path.append('/cheikh-w-flight-radar/src/main/it/exalt/katas/schema')
from RawProperties import raw_schema
sys.path.append('/cheikh-w-flight-radar/src/main/it/exalt/katas/writing')
from Writing import writeRawData



def alimRaw(spark: SparkSession):
    #Load Data
    flights_data = load_flight_data()

    #Convert data to spark Dataframe
    df_raw = toSparkDF(flights_data, raw_schema, spark)

    #Save data
    writeRawData(df_raw, 'append', 'avro', 'Flights/rawzone/')

    #Save data to reuse it for optimize Zone
    writeRawData(df_raw, 'overwrite', 'parquet', 'path_for_optimize_zone/')


