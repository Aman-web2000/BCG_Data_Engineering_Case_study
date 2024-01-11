from pyspark.sql import SparkSession
from Class_car_crash import CarCrashAnalytics



    # Take input and output paths from the user
config = {
            'primary_person_use_path':"Data/Primary_Person_use.csv",
            'units_use_path' : "Data/Units_use.csv",
            'damamges_use_path' : "Data/Damages_use.csv",
            'charges_use_path' : "Data/Charges_use.csv",
            'restrict_use_path' : "Data/Restrict_use.csv",
            'endorse_use_path' : "Data/Endorse_use.csv"
        }

    # Creating Spark session
spark = SparkSession.builder.appName("Car_Crash_CaseStudy").getOrCreate()

    # Creating CarCrashAnalytics instance
car_crash_analytics = CarCrashAnalytics(spark, config)

    # Running the analysis
car_crash_analytics.run_analysis()
