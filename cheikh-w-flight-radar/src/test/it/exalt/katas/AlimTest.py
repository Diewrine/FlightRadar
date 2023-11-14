import unittest
sys.path.append('/cheikh-w-flight-radar/src/main/it/exalt/katas/schema')
from RawProperties import raw_schema


from pyspark.sql import SparkSession
import sys
sys.path.append('C:/Users/SMARPOINT/Desktop/Project/cheikh-w-flight-radar/cheikh-w-flight-radar/src/main/it/exalt/katas/common')
from CommonMethods import HorodateFile

sys.path.append('C:/Users/SMARPOINT/Desktop/Project/cheikh-w-flight-radar/cheikh-w-flight-radar/src/main/it/exalt/katas/transformation')
from Transformation import instruction_1, instruction_2, instruction_3, instruction_4, instruction_5



class AlimTest(unittest.TestCase):

    spark = SparkSession.builder().master("yarn")\
        .appName("it.exalt.it")\
        .getOrCreate()

    df_test = spark.read.format('csv')\
        .schema(raw_schema)\
        .load("cheikh-w-flight-radar/src/test/resources/sampleFlightRadar.csv")


    def test_HorodateFile(self, df_test):
        self.assertEqual(HorodateFile(df_test), "20231110151031024")


    def test_instruction1(self, df_test, spark):
        self.assertEqual(instruction_1((df_test, spark).collect()[0][0]), "FedEx")

    def test_instruction2(self, df_test):
        self.assertEqual((instruction_2(df_test).select('airline_name').rdd.flatMap(lambda x: x).collect()) , ['Turkish Airlines', 'American Airlines'] )

    def test_instruction3(self, df_test):
        self.assertEqual((instruction_3(df_test).collect()[0][0]), "32cd1c32")

    def test_instruction4(self, df_test):
        self.assertEqual((instruction_4(df_test).select('distance_moyenne').rdd.flatMap(lambda x: x).collect()), [7316.035414444231, 10996.117595666117, 9395.258806685224, 11070.072050852568, 9450.209620141115, 10069.755292788188])

    def test_instruction5(self, df_test):
        self.assertEqual((instruction_5(df_test).collect()[0][0]), "Boeing")















