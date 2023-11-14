import sys
from pyspark.sql.connect.session import SparkSession

sys.path.append('C:/Users/SMARPOINT/Desktop/Project/cheikh-w-flight-radar/cheikh-w-flight-radar/src/main/it\exalt/katas/common')
from CommonMethods import load_safe, HorodateFile

sys.path.append('C:/Users/SMARPOINT/Desktop/Project/cheikh-w-flight-radar/cheikh-w-flight-radar/src/main/it\exalt/katas/transformation')
from Transformation import instruction_1, instruction_2, instruction_3, instruction_4, instruction_5

sys.path.append('/cheikh-w-flight-radar/src/main/it/exalt/katas/writing')
from Writing import WriteSafeData



def alimSafe(spark: SparkSession):
    #Load Data
    flights_data = load_safe(spark, "path_for_safe_zone/")
    result_1 = instruction_1(flights_data, spark)
    result_2 = instruction_2(flights_data)
    result_3 = instruction_3(flights_data)
    result_4 = instruction_4(flights_data)
    result_5 = instruction_5(flights_data)

    #Save all transformed data

    #for result_1
    WriteSafeData(result_1, 'append', 'avro', HorodateFile(result_1), 'Flights/SafeZone_res_1/')
    #for result_2
    WriteSafeData(result_2, 'append', 'avro', HorodateFile(result_2), 'Flights/SafeZone_res_2/')
    #for result_3
    WriteSafeData(result_3, 'append', 'avro', HorodateFile(result_3), 'Flights/SafeZone_res_3/')
    #for result_4
    WriteSafeData(result_4, 'append', 'avro', HorodateFile(result_4), 'Flights/SafeZone_res_4/')
    #for result_5
    WriteSafeData(result_5, 'append', 'avro', HorodateFile(result_5), 'Flights/SafeZone_res_5/')
