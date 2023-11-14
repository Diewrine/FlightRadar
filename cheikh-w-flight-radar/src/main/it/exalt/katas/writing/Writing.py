

def writeRawData(df, mode:str, format:str, path:str):
    df.write.mode(mode) \
        .format(format)\
        .save(path + "\\flightsRaw")



def WriteOptimizeData(df, mode:str, format:str, file_file_horodate:str, path:str):
    df.write.partitionBy("tech_year", "tech_month", "tech_day")\
        .mode(mode) \
        .format(format)\
        .save(path + "\\flights" + file_file_horodate)


def WriteSafeData(df, mode:str, format:str, file_file_horodate:str, path:str):
    df.write.partitionBy("tech_year", "tech_month", "tech_day") \
        .mode(mode) \
        .format(format) \
        .save(path + "\\flights" + file_file_horodate)