from pyspark.sql import SparkSession


from AlimRaw import alimRaw
from AlimOptimize import alimOptimize
from AlimSafe import alimSafe



def main():
    spark = SparkSession \
        .builder \
        .master("yarn")\
        .appName("cmw_flight_radar") \
        .getOrCreate()

    alimFuncs = [alimRaw, alimOptimize, alimSafe]
    for alim in alimFuncs:
        try:
            alim(spark)
        except Exception as e:
            print("Une erreur lors de l'alimentation du datalake")
            break


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()




