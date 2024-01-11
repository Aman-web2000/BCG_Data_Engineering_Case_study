# car_crash_analytics.py

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, sum, count, row_number, rank, upper
from pyspark.sql.window import Window
from typing import List, Dict
import os
import sys
import csv

class CarCrashAnalytics:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def load_data(self, file_path, header=True, infer_schema=True):
        return self.spark.read.csv(file_path, header=header, inferSchema=infer_schema)

    def count_male_killed(self, primary_person_df):
        """Analysis 1: Find the number of crashes with more than 2 males killed."""
        male_mask = primary_person_df['PRSN_GNDR_ID'] == 'MALE'
        kill_mask = primary_person_df['PRSN_INJRY_SEV_ID'] == 'KILLED'

        df1 = primary_person_df.filter((male_mask) & (kill_mask)).groupBy('CRASH_ID').agg(
            sum('DEATH_CNT').alias("TOTAL_DEATH")
        )

        result = df1.filter(df1['TOTAL_DEATH'] > 2).count()
        
        return result 
    
    def count_two_wheelers(self, units_df):
        """Analysis 2: Count of two wheelers involved in crashes."""
        
        df_two_wheelers = units_df.filter(units_df['VEH_BODY_STYL_ID'].like("%MOTORCYCLE%"))
        result = df_two_wheelers.count()
        return result
    
    def count_airbags_not_deployed(self, primary_person_df, units_df):
        """3.	Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy."""
        
        kill_mask = primary_person_df['PRSN_INJRY_SEV_ID'] == 'KILLED'
        driver_car_mask = primary_person_df['PRSN_TYPE_ID']=='DRIVER'
        airbag_mask = primary_person_df['PRSN_AIRBAG_ID']=='NOT DEPLOYED'
        
        lis_of_carsh_id = primary_person_df.filter((driver_car_mask) & (kill_mask) & (airbag_mask)).select('CRASH_ID')

        car_list= ['PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR', 'VAN', 'POLICE CAR/TRUCK', 'NEV-NEIGHBORHOOD ELECTRIC VEHICLE']

        df_car = units_df.filter(units_df['VEH_BODY_STYL_ID'].isin(car_list))
        df_joined = df_car.join(lis_of_carsh_id,"CRASH_ID",'inner')
        df_joined = df_joined.groupBy("VEH_MAKE_ID").agg(count("CRASH_ID").alias("count"))
        result =  df_joined.orderBy(col("count").desc()).limit(5)   
        return result
    
    def count_hit_run(self, primary_person_df, units_df):
        """Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? """

        ## Creating mask for desired conditions
        driver_mask = primary_person_df['PRSN_TYPE_ID'].isin(['DRIVER', 'DRIVER OF MOTORCYCLE TYPE VEHICLE'])
        license_mask = primary_person_df['DRVR_LIC_TYPE_ID'].isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.'])
        license_valid_mask = ~primary_person_df['DRVR_LIC_CLS_ID'].isin(['UNLICENSED', 'UNKNOWN'])

        ## crash ids with valid licences
        df_lic = primary_person_df.filter((driver_mask) & (license_mask) & (license_valid_mask)).select('CRASH_ID').distinct()

        ## hit and run data
        df_hit_and_run = units_df.filter(units_df['VEH_HNR_FL']=='Y')

        ## filtering data for valid licence and hit and run cases
        result = df_hit_and_run.join(df_lic, 'CRASH_ID', 'inner').agg(count("CRASH_ID").alias("count"))

        return result

    def count_not_female_states(self, primary_person_df):
        """Analysis 5: Which state has highest number of accidents in which females are not involved?"""
        
        df_female = primary_person_df.filter(primary_person_df['PRSN_GNDR_ID']=='FEMALE').select('CRASH_ID').distinct()

        df_not_female = primary_person_df.join(df_female,primary_person_df['CRASH_ID']==df_female['CRASH_ID'], "left_anti")

        df_state_count = df_not_female.groupBy("DRVR_LIC_STATE_ID").agg(count('CRASH_ID').alias("crash_count"))

        result = df_state_count.orderBy(col("crash_count").desc()).limit(1)
        
        return result
    
    def vech_make_id_3_5(self, primary_person_df, units_df):
        """Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death"""
        
        ## mask for non injury 
        non_injury = primary_person_df['PRSN_INJRY_SEV_ID'].isin(['NOT INJURED', 'UNKNOWN'])
        ## Fetching unique crash_id data for all kind of injury and death
        df_injury_death = primary_person_df.filter(~non_injury).select("CRASH_ID").distinct()
        ## joining the data for injury and death with units
        df_unit_injury = units_df.join(df_injury_death, "CRASH_ID", 'inner')

        df1 = df_unit_injury.na.drop().groupBy("VEH_MAKE_ID").agg(count("CRASH_ID").alias("count_crash"))

        df1 = df1.orderBy(col('count_crash').desc())

        window_spec = Window.orderBy(col("count_crash").desc())

        df1 = df1.withColumn("row_num", row_number().over(window_spec))

        result = df1.filter((col("row_num")>=3) & (col('row_num')<=5)).drop("row_num")
        
        return result
    
    def ethinicity_vech_body(self, primary_person_df, units_df):
        """Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style"""
        
        merged_df = primary_person_df.join(units_df, "CRASH_ID", 'inner')

        merged_df1 = merged_df.select(['VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID']).groupBy(['VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID']).agg(count('PRSN_ETHNICITY_ID').alias("count_ethnicity"))

        window_eth = Window.orderBy(merged_df1['count_ethnicity'].desc()).partitionBy(merged_df1['VEH_BODY_STYL_ID'])

        merged_df2 = merged_df1.withColumn("rank", rank().over(window_eth))

        result = merged_df2.filter(merged_df2['rank']==1).drop('rank')

        return result
    
    def top_5_alc_zip_codes(self, primary_person_df, units_df):
        """Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)"""
        
        car_list= ['PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR', 'VAN', 'POLICE CAR/TRUCK', 'NEV-NEIGHBORHOOD ELECTRIC VEHICLE']
        
        merged_df = primary_person_df.join(units_df, "CRASH_ID", 'inner')
        
        ## alcohol mask
        alcohol_mask = merged_df['PRSN_ALC_RSLT_ID']=='Positive'

        merged_df.select(['VEH_BODY_STYL_ID', 'DRVR_ZIP']).show()

        car_clc_df = merged_df.filter((alcohol_mask) & (merged_df['VEH_BODY_STYL_ID'].isin(car_list))).select(['CRASH_ID', 'DRVR_ZIP'])

        car_clc_df1 = car_clc_df.na.drop().groupBy('DRVR_ZIP').agg(count("CRASH_ID").alias("crash_count")).orderBy(col("crash_count").desc())

        result = car_clc_df1.limit(5)

        return result
    
    def count_no_dam_insur(self, units_df, damage_df):
        """Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance"""
        
        car_list= ['PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR', 'VAN', 'POLICE CAR/TRUCK', 'NEV-NEIGHBORHOOD ELECTRIC VEHICLE']
        
        ## crash_id with insurance
        df_insurance = units_df.select("FIN_RESP_TYPE_ID","CRASH_ID").filter(col("FIN_RESP_TYPE_ID").like("%INSURANCE")).select("CRASH_ID").distinct()

        ## crash id with VEH_DMAG_SCL >4 :
        VEH_DMAG_SCL_lis = ["DAMAGED 6", "DAMAGED 5", "DAMAGED 7 HIGHEST"]

        df_veh_dmag = units_df.filter((units_df['VEH_DMAG_SCL_1_ID'].isin(VEH_DMAG_SCL_lis)) & (units_df['VEH_DMAG_SCL_2_ID'].isin(VEH_DMAG_SCL_lis))).select("CRASH_ID").distinct()

        # Identify the distinct CRASH_IDs in units_df that are not in damage_df

        crash_ids_to_include= units_df.join(damage_df, units_df['CRASH_ID']==damage_df['CRASH_ID'], "left_anti").filter(col("FIN_RESP_TYPE_ID").like("%INSURANCE") & (units_df['VEH_DMAG_SCL_1_ID'].isin(VEH_DMAG_SCL_lis)) & (units_df['VEH_DMAG_SCL_2_ID'].isin(VEH_DMAG_SCL_lis)))

        result = crash_ids_to_include.select("CRASH_ID").distinct().count()
        
        return result
    
    def vech_make_speeding(self, primary_person_df, units_df, charges_df):
        """Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)"""
        
        car_list= ['PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR', 'VAN', 'POLICE CAR/TRUCK', 'NEV-NEIGHBORHOOD ELECTRIC VEHICLE']
        
        license_mask = primary_person_df['DRVR_LIC_TYPE_ID'].isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.'])
        license_valid_mask = ~primary_person_df['DRVR_LIC_CLS_ID'].isin(['UNLICENSED', 'UNKNOWN'])

        ## fetching data for car licensed with the with Top 25 states with highest number of offences:
        df_top_25 = units_df.filter(units_df['VEH_BODY_STYL_ID'].isin(car_list)).select('VEH_LIC_STATE_ID',"CRASH_ID").groupby('VEH_LIC_STATE_ID').agg(count("CRASH_ID").alias("lic_count")).orderBy(col("lic_count").desc())

        df_top_25 = df_top_25.na.drop().limit(25)

        df_top_colors = units_df.filter(units_df['VEH_BODY_STYL_ID'].isin(car_list)).select("VEH_COLOR_ID","CRASH_ID").groupBy("VEH_COLOR_ID").agg(count("CRASH_ID").alias("count_color")).orderBy(col("count_color").desc())

        df_top_colors = df_top_colors.limit(10)

        ## data with top 10 colors and top 25 states 
        merge_df = units_df.join(df_top_25, "VEH_LIC_STATE_ID", "inner")
        merge_df1 = merge_df.join(df_top_colors, "VEH_COLOR_ID", "inner")
        df_distinct_id = merge_df1.select("CRASH_ID", "VEH_COLOR_ID", "VEH_LIC_STATE_ID").select("CRASH_ID").distinct()

        ## fetching data for speeding related offences
        charges_df = charges_df.withColumn("charges", upper(col("CHARGE")))
        df_crash_speed_id = charges_df.filter(col("charges").like("%SPEED%")).select("CRASH_ID").distinct()

        ## getting the crash id with licenced driver
        df_lic_driver = primary_person_df.filter((license_valid_mask) & (license_mask)).select("CRASH_ID").distinct()

        ## Fetching the data with all the required filters
        result = units_df.join(df_lic_driver, "CRASH_ID", "inner").join(df_crash_speed_id,"CRASH_ID", "inner").join(df_distinct_id, "CRASH_ID", "inner").select("VEH_MAKE_ID","CRASH_ID").groupBy("VEH_MAKE_ID").agg(count("CRASH_ID").alias("vech_make_count")).orderBy(col("vech_make_count").desc()).limit(5)

        return result

    def save_txt(self, output_file, result):
        """save the output in a txt file inside output_file
        output_file: output_file_path/output_file_name
        result : value to store"""
               
        output_directory = os.path.dirname(output_file)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
                        
        # Save result to output path as CSV
        my_variable = [("count", result)]
        # Save variable to .csv file
        with open(output_file, "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(my_variable)
            
        

    def run_analysis(self):
        # Load data using input path
        
        primary_person_df = self.load_data(self.config['primary_person_use_path'])
        units_df = self.load_data(self.config['units_use_path'])
        damage_df = self.load_data(self.config['damamges_use_path'])
        charges_df = self.load_data(self.config['charges_use_path'])
        restrict_df = self.load_data(self.config['restrict_use_path'])
        endorse_df = self.load_data(self.config['endorse_use_path'])
        
        
        # count_male_killed
        result = self.count_male_killed(primary_person_df)
        print("number of crashes (accidents) in which number of males killed are greater than 2:")
        print(result)
        output_file = "Output/output_count_male_killed.csv"
        
        # Save result to output path as CSV
        self.save_txt(output_file, result)
        
        # count_two_wheelers
        result = self.count_two_wheelers(units_df)
        print("number of crashes (accidents) in which number of males killed are greater than 2:")
        print(result)
        output_file = "Output/output_count_two_wheelers.csv"
        
        self.save_txt(output_file, result)
            
        # count_airbags_not_deployed
        result = self.count_airbags_not_deployed(primary_person_df, units_df)
        print("Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy")
        result.show()
        output_file = "Output/output_count_airbags_not_deployed.csv"
        # Save result to output path as CSV
        result.write.csv(output_file, header=True, mode='overwrite')
        
        # count_hit_run
        result = self.count_hit_run(primary_person_df, units_df)
        print("number of Vehicles with driver having valid licences involved in hit and run")
        print(result)
        output_file = "Output/output_count_hit_run.csv"
        # Save result to output path as CSV
        self.save_txt(output_file, result)
        
        # count_not_female_states
        result = self.count_not_female_states(primary_person_df)
        print("state has highest number of accidents in which females are not involved")
        result.show()
        output_file = "Output/output_count_not_female_states.csv"
        # Save result to output path as CSV
        result.write.csv(output_file, header=True, mode='overwrite')
        
        # vech_make_id_3_5
        result = self.vech_make_id_3_5(primary_person_df, units_df)
        print("the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death")
        result.show()
        output_file = "Output/output_vech_make_id_3_5.csv"
        # Save result to output path as CSV
        result.write.csv(output_file, header=True, mode='overwrite')
        
        # ethinicity_vech_body
        result = self.ethinicity_vech_body(primary_person_df, units_df)
        print("body styles involved in crashes, mention the top ethnic user group of each unique body style1")
        result.show()
        output_file = "Output/output_ethinicity_vech_body.csv"
        # Save result to output path as CSV
        result.write.csv(output_file, header=True, mode='overwrite')
        
        # top_5_alc_zip_codes
        result = self.top_5_alc_zip_codes(primary_person_df, units_df)
        print("Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)")
        result.show()
        output_file = "Output/output_top_5_alc_zip_codes.csv"
        # Save result to output path as CSV
        result.write.csv(output_file, header=True, mode='overwrite')
        
        # count_no_dam_insur
        result = self.count_no_dam_insur(units_df, damage_df)
        print("Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance")
        print(result)
        output_file = "Output/output_count_no_dam_insur.csv"
        # Save result to output path as CSV
        self.save_txt(output_file, result)
        
        # vech_make_speeding
        result = self.vech_make_speeding(primary_person_df, units_df, charges_df)
        print("Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance")
        result.show()
        output_file = "Output/output_vech_make_speeding.csv"
        # Save result to output path as CSV
        result.write.csv(output_file, header=True, mode='overwrite')
        
        
        
        
        
        
