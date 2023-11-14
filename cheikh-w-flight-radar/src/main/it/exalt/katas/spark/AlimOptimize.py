import sys
from pyspark.sql.connect.session import SparkSession

sys.path.append('C:/Users/SMARPOINT/Desktop/Project/cheikh-w-flight-radar/cheikh-w-flight-radar/src/main/it/exalt/katas/common')
from CommonMethods import load_optimize, EnrichData, HorodateFile

sys.path.append('/cheikh-w-flight-radar/src/main/it/exalt/katas/writing')
from Writing import WriteOptimizeData



def alimOptimize(spark: SparkSession):
    #Load Data
    flights_data = load_optimize(spark, "path_for_optimize_zone/")

    #Enrich Data
    df_optimize = EnrichData(flights_data)

    #Retrieve horodate
    file_horodate = HorodateFile(flights_data)

    #Save data
    WriteOptimizeData(df_optimize, 'append', 'avro', file_horodate, 'Flights/optimizeZone/')

    #Save data to reuse it for safe zone
    WriteOptimizeData(df_optimize, 'overwrite', 'parquet', file_horodate, 'path_for_safe_zone/')
