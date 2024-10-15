from pyspark.sql import Window
from pyspark.sql.functions import col, sum, first, count, row_number, lower, desc


# The `Analyser` class in Python contains methods to perform various analyses on different dataframes
# related to crash data.
class Analyser:
    def __init__(self, spark, charges_df, damages_df, endorse_df, primary_person_df, restrict_df, units_df):
        self.spark = spark
        self.charges_df = charges_df
        self.damages_df = damages_df
        self.endorse_df = endorse_df
        self.primary_person_df = primary_person_df
        self.restrict_df = restrict_df
        self.units_df = units_df

    def analysis_one(self):
        """
        The function `analysis_one` filters and analyzes data to count the number of crashes where more
        than 2 males were killed.
        """

        # Step 1: Filter records where the gender is male and death count is greater than zero
        males_killed_df = self.primary_person_df.filter((col("PRSN_GNDR_ID") == "MALE") & (
            self.primary_person_df['PRSN_INJRY_SEV_ID'] == "KILLED"))

        # Step 2: Group by CRASH_ID and sum the DEATH_CNT
        males_killed_by_crash_df = males_killed_df.groupBy(
            "CRASH_ID").agg(sum("DEATH_CNT").alias("total_male_deaths"))

        # Step 3: Filter the crashes where the number of male deaths is greater than 2
        crashes_with_more_than_two_males_killed_df = males_killed_by_crash_df.filter(
            col("total_male_deaths") > 2)

        # Step 4: Count the number of distinct CRASH_IDs in above df
        number_of_crashes = crashes_with_more_than_two_males_killed_df.select(
            "CRASH_ID").count()

        # Display the result
        print(
            f"The number of crashes in which more than 2 males were killed are: {number_of_crashes}")

        # Convert the result to a DataFrame
        output_df = self.spark.createDataFrame(
            [(number_of_crashes,)], ["Number_of_Crashes_2+_male_deaths"])

        return output_df

# spark.stop()

    def analysis_two(self):
        """
        The function `analysis_two` filters for two-wheelers in a DataFrame, counts the number of unique
        crashes involving two-wheelers, and returns the result in a DataFrame.
        :return: The function `analysis_two` returns a DataFrame containing the count of unique crashes
        involving two-wheelers. The DataFrame has one column named "Number_of_Two_Wheelers_Crashed"
        which stores the count of two-wheeler crashes.
        """
        # Step 1: Filter for two-wheelers
        two_wheelers_df = self.units_df.filter(col("VEH_BODY_STYL_ID").isin(
            ["MOTORCYCLE", "POLICE MOTORCYCLE"])).select("CRASH_ID")

        # Step 2: Count the number of unique crashes
        number_of_two_wheeler_crashes = two_wheelers_df.count()

        # Display the result
        print(
            f"The number of Two Wheelers that are booked for crashes: {number_of_two_wheeler_crashes}")

        # Convert the result to a DataFrame
        output_df = self.spark.createDataFrame([(number_of_two_wheeler_crashes,)], [
                                               "Number_of_Two_Wheelers_Crashed"])

        return output_df

    def analysis_three(self):
        """
        The function `analysis_three` filters and analyzes data to identify the top 5 vehicle makes
        involved in driver deaths with no airbags deployed.
        :return: The `analysis_three` function returns a DataFrame containing the top 5 vehicle makes
        involved in fatal crashes where the driver was killed and no airbags were deployed. The
        DataFrame includes columns for the vehicle make (`VEH_MAKER`) and the number of vehicles
        involved in such crashes (`Number_of_Vehicles`).
        """
        # Step 1: Filter the Primary_Person data for driver deaths with no airbags deployed
        driver_deaths_df = self.primary_person_df.filter((col("PRSN_TYPE_ID") == "DRIVER") & (self.primary_person_df['PRSN_INJRY_SEV_ID'] == "KILLED") & (
            self.primary_person_df['PRSN_AIRBAG_ID'] == "NOT DEPLOYED")).select("CRASH_ID", "UNIT_NBR")

        # Step 2: Join the filtered Primary_Person data with the Units data
        joined_df = driver_deaths_df.join(self.units_df, (driver_deaths_df.CRASH_ID == self.units_df.CRASH_ID) & (
            driver_deaths_df.UNIT_NBR == self.units_df.UNIT_NBR), "inner").select("VEH_BODY_STYL_ID", "VEH_MAKE_ID")

        # Step 3: Filter for cars only
        cars_without_airbag_deployment_df = joined_df.filter(col("VEH_BODY_STYL_ID").isin(
            ["PASSENGER CAR, 4-DOOR", "SPORT UTILITY VEHICLE", "PASSENGER CAR, 2-DOOR", "VAN", "POLICE CAR/TRUCK"]))

        # Step 4: Group by VEH_MAKE_ID, count occurrences, and sort to get top 5
        top5_vehicle_makes_df = cars_without_airbag_deployment_df.groupBy(
            "VEH_MAKE_ID").count().orderBy(col("count").desc()).limit(5)
        top5_vehicle_makes_df = top5_vehicle_makes_df.select(
            col("VEH_MAKE_ID").alias("VEH_MAKER"), col("count").alias("Number_of_Vehicles"))

        return top5_vehicle_makes_df

    def analysis_four(self):
        """
        The function `analysis_four` filters data for valid licenses, hit-and-run incidents, joins the
        data, counts the number of vehicles involved, and returns the result.
        :return: The `analysis_four` method is returning a DataFrame containing the count of vehicles
        that are involved in hit-and-run incidents but driven by licensed drivers. The count is
        calculated based on the filtered and joined data from the Primary_Person and Units DataFrames.
        The result is displayed as a message and saved to a new DataFrame named `result_df`, which is
        then returned by the method.
        """
        # Step 1: Filter the Primary_Person data for valid licenses
        valid_license_df = self.primary_person_df.filter(col("DRVR_LIC_TYPE_ID").isin(
            ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])).select("CRASH_ID", "UNIT_NBR", "DRVR_LIC_TYPE_ID")

        # Step 2: Filter the Units data for hit-and-run incidents
        hit_and_run_df = self.units_df.filter(col("VEH_HNR_FL") == "Y").select(
            "CRASH_ID", "UNIT_NBR", "VEH_HNR_FL")

        # Step 3: Join the filtered data on CRASH_ID and UNIT_NBR
        joined_df = valid_license_df.join(hit_and_run_df, (valid_license_df.CRASH_ID == hit_and_run_df.CRASH_ID) & (
            valid_license_df.UNIT_NBR == hit_and_run_df.UNIT_NBR), "inner")

        # Step 4: Count the number of vehicles involved
        vehicle_count = joined_df.count()

        # Display the result
        print(
            f"The number of Vehicles that are involved with Hit and Run but driven by Licensed Drivers: {vehicle_count}")

        # Save the result to dataframe
        result_df = self.spark.createDataFrame(
            [(vehicle_count,)], ["Number_of_Vehicles_HnR_Licensed"])

        return result_df

    def analysis_five(self):
        """
        This function analyzes crash data to find the state with the highest number of crashes involving
        individuals who are not female.
        :return: The `analysis_five` function returns a DataFrame containing the state with the highest
        number of crashes where females are not involved as drivers. The DataFrame has two columns:
        "STATE_with_max_no_F_crashes" which represents the state with the highest number of crashes, and
        "Number_of_Crashes" which represents the count of crashes in that state.
        """
        # Step 1: Filter out records where the person is a female
        females_df = self.primary_person_df.filter(
            col("PRSN_GNDR_ID") == "FEMALE")

        # Step 2: List out crashes where females are involved
        crashes_female_df = females_df.select("CRASH_ID").distinct()

        # Step 3: Filter out crashes involving females from the original data
        no_female_crashes_df = self.primary_person_df.join(
            crashes_female_df, on="CRASH_ID", how="left_anti")

        # Step 4: Group by CRASH_ID and get state (driver's state)
        crashes_by_state = no_female_crashes_df.groupBy("CRASH_ID").agg(
            first("DRVR_LIC_STATE_ID").alias("DRIVER_STATE"))

        # Step 5: Count number of crashes grouped by state
        state_crash_count = crashes_by_state.groupBy("DRIVER_STATE").count()

        # Step 6: Find the state with the highest number of crashes
        state_with_max_crashes = state_crash_count.orderBy(
            col("count").desc()).limit(1)
        state_with_max_crashes = state_with_max_crashes.select(col("DRIVER_STATE").alias(
            "STATE_with_max_no_F_crashes"), col("count").alias("Number_of_Crashes"))

        return state_with_max_crashes

    def analysis_six(self):
        """
        The function `analysis_six` calculates and returns the VEH_MAKE_IDs of the 3rd to 5th highest
        total injuries from a given dataset.
        :return: The `analysis_six` function returns a DataFrame containing the VEH_MAKE_IDs of the 3rd
        to 5th highest total injuries, sorted in descending order based on the total injuries.
        """
        # Step 1: Calculate the total number of injuries including deaths
        injuries_df = self.units_df.withColumn("TOTAL_INJURIES", col("TOT_INJRY_CNT") + col("DEATH_CNT")).select(
            "CRASH_ID", "UNIT_NBR", "VEH_MAKE_ID", "TOT_INJRY_CNT", "DEATH_CNT", "TOTAL_INJURIES")

        # Step 2: Group by VEH_MAKE_ID and sum the TOTAL_INJURIES
        vehicle_injury_counts = injuries_df.groupBy("VEH_MAKE_ID").agg(
            sum("TOTAL_INJURIES").alias("TOTAL_INJURIES_SUM"))

        # Step 3: Sort the VEH_MAKE_IDs by total injuries in descending order
        sorted_vehicle_injuries = vehicle_injury_counts.orderBy(
            col("TOTAL_INJURIES_SUM").desc())

        # Step 4: Select the 3rd to 5th VEH_MAKE_IDs
        top_3rd_to_5th_vehicle_makes = sorted_vehicle_injuries.limit(5).tail(3)

        # Output the result
        top_3rd_to_5th_vehicle_makes_df = self.spark.createDataFrame(
            top_3rd_to_5th_vehicle_makes)

        return top_3rd_to_5th_vehicle_makes_df

    def analysis_seven(self):
        """
        The function `analysis_seven` performs analysis on vehicle body styles and person ethnicities to
        determine the top ethnic group for each body style.
        :return: The `analysis_seven` method returns the top ethnic group for each vehicle body style
        based on the count of occurrences in the joined dataframes `units_df` and `primary_person_df`.
        """
        # Step 1: Join units_df and primary_person_df on CRASH_ID and UNIT_NBR
        joined_df = self.units_df.join(self.primary_person_df, on=[
                                       "CRASH_ID", "UNIT_NBR"], how="inner")

        # Step 2: Group by VEH_BODY_STYL_ID and PRSN_ETHNICITY_ID, and count occurrences
        ethnic_group_counts = joined_df.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").agg(
            count("*").alias("ETHNIC_GROUP_COUNT"))

        # Step 3: Use Window function to rank ethnic groups for each body style
        window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(
            col("ETHNIC_GROUP_COUNT").desc())
        ranked_ethnic_groups = ethnic_group_counts.withColumn(
            "rank",
            row_number().over(window_spec)
        )

        # Step 4: Filter to get the top ethnic group for each body style (rank = 1)
        top_ethnic_user_group = ranked_ethnic_groups.filter(
            col("rank") == 1).select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")

        return top_ethnic_user_group

    def analysis_eight(self):
        """
        The function `analysis_eight` filters and analyzes data to identify the top 5 zip codes with the
        highest number of alcohol-related crashes involving drivers.
        :return: The `analysis_eight` method returns a DataFrame containing the top 5 zip codes with the
        highest number of alcohol-related crashes involving drivers.
        """
        # Step 1: Filter the data for alcohol as a contributing factor
        alcohol_related_crashes = self.primary_person_df.filter((col("PRSN_ALC_RSLT_ID") == "Positive") & (
            col("PRSN_TYPE_ID") == "DRIVER")).select("CRASH_ID", "PRSN_ALC_RSLT_ID", "PRSN_TYPE_ID", "DRVR_ZIP")

        # Step 2: Group by the driver's zip code and count the number of crashes
        crash_count_by_zip = alcohol_related_crashes.groupBy(
            "DRVR_ZIP").agg(count("CRASH_ID").alias("CRASH_COUNT"))

        # Step 3: Sort by crash count in descending order and select the top 5 zip codes
        top_5_zip_codes = crash_count_by_zip.orderBy(
            col("CRASH_COUNT").desc()).limit(6).tail(5)

        # Output the result
        top_5_zip_codes_df = self.spark.createDataFrame(top_5_zip_codes)

        return top_5_zip_codes_df

    def analysis_nine(self):
        """
        The function `analysis_nine` filters and analyzes records of damaged vehicles with insurance
        coverage to count distinct crash IDs meeting specific criteria.
        :return: The `analysis_nine` function is returning a DataFrame named `output_df` that contains
        the count of distinct Crash IDs that meet all the specified criteria in the analysis steps. The
        DataFrame has one column named "Number_of_Crashes" which holds the count of distinct Crash IDs.
        """
        # Step 1: Filter for records with damaged property observed
        # it is damaged_df database

        # Step 2: Filter for damage levels above 4 (DAMAGED 5 or higher)
        damage_level_df = self.units_df.filter((col("VEH_DMAG_SCL_1_ID").isin(["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"])) | (
            col("VEH_DMAG_SCL_2_ID").isin(["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"])))

        # Step 3: Join no_damaged_property_df with damage_level_df
        damage_and_no_property_df = damage_level_df.join(
            self.damages_df, "CRASH_ID", "left_anti")

        # Step 4: Filter for cars with insurance
        damaged_insured_df = damage_and_no_property_df.filter(col("FIN_RESP_TYPE_ID").isin(
            ["PROOF OF LIABILITY INSURANCE", "LIABILITY INSURANCE POLICY"]))

        # Step 5: Count distinct CRASH_IDs
        distinct_crash_count = damaged_insured_df.select(
            "CRASH_ID").distinct().count()

        print("Count of distinct Crash IDs meeting all criteria:",
              distinct_crash_count)

        # Convert the result to a DataFrame
        output_df = self.spark.createDataFrame(
            [(distinct_crash_count,)], ["Number_of_Crashes"])

        return output_df

    def analysis_ten(self):
        """
        The function `analysis_ten` performs data analysis to identify the top 5 vehicle makes
        associated with speeding offenses by licensed drivers in specific states with the highest number
        of offenses.
        :return: The `analysis_ten` method returns the top 5 vehicle makes based on the specified
        criteria and data processing steps outlined in the code.
        """
        # Step 1: Filter for speeding-related offences
        speeding_offences_df = self.charges_df.filter(
            lower(col("CHARGE")).contains("speed"))

        # Step 2: Filter for licensed drivers
        licensed_drivers_df = self.primary_person_df.filter(
            col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))

        # Step 3: Find the top 10 most common vehicle colors
        top_10_colors = self.units_df.groupBy(
            "VEH_COLOR_ID").count().orderBy(desc("count")).limit(11)
        top_10_color_list = [row["VEH_COLOR_ID"]
                             for row in top_10_colors.collect() if row["VEH_COLOR_ID"] != 'NA']

        # Step 4: Determine the top 25 states with the highest number of offences
        # Aggregate the charges by CRASH_ID
        charges_agg = self.charges_df.groupBy("CRASH_ID").count()

        #  Join with primary_person_df to get the driver's license state
        offenses_by_state = charges_agg.join(self.primary_person_df, on="CRASH_ID", how="inner").groupBy(
            "DRVR_LIC_STATE_ID").agg(count("CRASH_ID").alias("offense_count")).orderBy(desc("offense_count")).limit(28)

        top_25_states_list = [row.DRVR_LIC_STATE_ID for row in offenses_by_state.filter(
            ~col("DRVR_LIC_STATE_ID").isin("NA", "Other", "Unknown")).collect()]

        # Step 5: Filter units_df for cars with one of the top 10 colors
        filtered_units_df = self.units_df.filter(
            (col("VEH_COLOR_ID").isin(top_10_color_list)))

        # Step 6: Join data to get licensed drivers with speeding offences and License stste in top 25 states
        filtered_drivers_df = speeding_offences_df.join(licensed_drivers_df, on="CRASH_ID", how="inner").filter(
            col("DRVR_LIC_STATE_ID").isin(top_25_states_list))

        # Step 7: Join above two dataframes to get required dataframe
        final_df = filtered_drivers_df.join(
            filtered_units_df, on=["CRASH_ID", "UNIT_NBR"], how="inner")

        # Step 5: Determine the top 5 vehicle makes
        top_vehicle_makes = final_df.groupBy("VEH_MAKE_ID").agg(
            count("CRASH_ID").alias("offense_count")).orderBy(desc("offense_count")).limit(5)

        return top_vehicle_makes
