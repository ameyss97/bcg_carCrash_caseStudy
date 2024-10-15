import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from utils.config_loader import ConfigLoader


# The `DataLoader` class in Python defines methods to load different files of data with specific
# schemas using Apache Spark.
class DataLoader:
    def __init__(self, spark):
        self.spark = spark
        self.input_path = os.path.join(
            os.getcwd(), ConfigLoader.load_paths_config()["paths"]["input"])

    def load_charges(self):
        """
        The function `load_charges` reads a CSV file containing data about criminal Charges placed on persons with a specified schema
        using Apache Spark.
        :return: The `load_charges` method returns a DataFrame that is created by reading a CSV file
        located at the specified path `Charges_use.csv` with the defined schema `charges_schema`. The
        DataFrame is loaded using Apache Spark with the specified schema and options.
        """
        charges_path = os.path.join(self.input_path, "Charges_use.csv")
        charges_schema = StructType([
            StructField("CRASH_ID", IntegerType(), True),
            StructField("UNIT_NBR", IntegerType(), True),
            StructField("PRSN_NBR", IntegerType(), True),
            StructField("CHARGE", StringType(), True),
            StructField("CITATION_NBR", StringType(), True)
        ])
        return self.spark.read.format("csv").option("header", "true").schema(charges_schema).load(charges_path)

    def load_damages(self):
        """
        The function `load_damages` reads a CSV file containing information about Damages to properties and returns a
        DataFrame with specified schema.
        :return: The `load_damages` function is returning a DataFrame that is created by reading a CSV
        file located at "Damages_use.csv" within the `input_path` directory.
        """
        Damages_path = os.path.join(self.input_path, "Damages_use.csv")
        damages_schema = StructType([
            StructField("CRASH_ID", IntegerType(), True),
            StructField("DAMAGED_PROPERTY", StringType(), True)
        ])
        return self.spark.read.format("csv").option("header", "true").schema(damages_schema).load(Damages_path)

    def load_endorse(self):
        """
        The function `load_endorse` reads a CSV file containing endorsement data using a specified
        schema in a Spark environment.
        :return: The `load_endorse` function is returning a DataFrame that is created by reading a CSV
        file located at the path "Endorse_use.csv" within the `input_path` directory. The DataFrame is
        being created with a specific schema defined by the `endorse_schema`.
        """
        endorse_path = os.path.join(self.input_path, "Endorse_use.csv")
        endorse_schema = StructType([
            StructField("CRASH_ID", IntegerType(), True),
            StructField("UNIT_NBR", IntegerType(), True),
            StructField("DRVR_LIC_ENDORS_ID", StringType(), True)
        ])
        return self.spark.read.format("csv").option("header", "true").schema(endorse_schema).load(endorse_path)

    def load_primary_person(self):
        """
        The function `load_primary_person` reads a CSV file containing data about primary persons
        involved in crashes and returns a Spark DataFrame with a specified schema.
        :return: The `load_primary_person` function returns a DataFrame that is created by reading a CSV
        file located at the path "Primary_Person_use.csv" within the `input_path` directory. The
        DataFrame is structured according to the specified schema `primary_person_schema`, which defines
        the columns and their data types. The function uses Apache Spark to read the CSV file with the
        specified schema and returns the resulting DataFrame.
        """
        primary_person_path = os.path.join(
            self.input_path, "Primary_Person_use.csv")
        primary_person_schema = StructType([
            StructField("CRASH_ID", IntegerType(), True),
            StructField("UNIT_NBR", IntegerType(), True),
            StructField("PRSN_NBR", IntegerType(), True),
            StructField("PRSN_TYPE_ID", StringType(), True),
            StructField("PRSN_OCCPNT_POS_ID", StringType(), True),
            StructField("PRSN_INJRY_SEV_ID", StringType(), True),
            StructField("PRSN_AGE", FloatType(), True),
            StructField("PRSN_ETHNICITY_ID", StringType(), True),
            StructField("PRSN_GNDR_ID", StringType(), True),
            StructField("PRSN_EJCT_ID", StringType(), True),
            StructField("PRSN_REST_ID", StringType(), True),
            StructField("PRSN_AIRBAG_ID", StringType(), True),
            StructField("PRSN_HELMET_ID", StringType(), True),
            StructField("PRSN_SOL_FL", StringType(), True),
            StructField("PRSN_ALC_SPEC_TYPE_ID", StringType(), True),
            StructField("PRSN_ALC_RSLT_ID", StringType(), True),
            StructField("PRSN_BAC_TEST_RSLT", FloatType(), True),
            StructField("PRSN_DRG_SPEC_TYPE_ID", StringType(), True),
            StructField("PRSN_DRG_RSLT_ID", StringType(), True),
            StructField("DRVR_DRG_CAT_1_ID", StringType(), True),
            StructField("PRSN_DEATH_TIME", StringType(), True),
            StructField("INCAP_INJRY_CNT", IntegerType(), True),
            StructField("NONINCAP_INJRY_CNT", IntegerType(), True),
            StructField("POSS_INJRY_CNT", IntegerType(), True),
            StructField("NON_INJRY_CNT", IntegerType(), True),
            StructField("UNKN_INJRY_CNT", IntegerType(), True),
            StructField("TOT_INJRY_CNT", IntegerType(), True),
            StructField("DEATH_CNT", IntegerType(), True),
            StructField("DRVR_LIC_TYPE_ID", StringType(), True),
            StructField("DRVR_LIC_STATE_ID", StringType(), True),
            StructField("DRVR_LIC_CLS_ID", StringType(), True),
            StructField("DRVR_ZIP", StringType(), True)
        ])
        return self.spark.read.format("csv").option("header", "true").schema(primary_person_schema).load(primary_person_path)

    def load_restrict(self):
        """
        The function `load_restrict` reads a CSV file containing restrictions crash data with specific schema using
        Apache Spark.
        :return: The code is returning a DataFrame that is loaded from a CSV file located at the path
        "Restrict_use.csv". The DataFrame has a specified schema with three columns: "CRASH_ID" of type
        Integer, "UNIT_NBR" of type Integer, and "DRVR_LIC_RESTRIC_ID" of type StringType.
        """
        restrict_path = os.path.join(self.input_path, "Restrict_use.csv")
        restrict_schema = StructType([
            StructField("CRASH_ID", IntegerType(), True),
            StructField("UNIT_NBR", IntegerType(), True),
            StructField("DRVR_LIC_RESTRIC_ID", StringType(), True)
        ])
        return self.spark.read.format("csv").option("header", "true").schema(restrict_schema).load(restrict_path)

    def load_units(self):
        """
        The `load_units` function reads a CSV file containing data of vehicle units involved crashes with specific schema using
        Apache Spark.
        :return: The `load_units` method is returning a DataFrame that is created by reading a CSV file
        located at the path specified by `units_path`. The DataFrame is created using a specified schema
        `units_schema` which defines the structure of the data in the CSV file. The method uses Apache
        Spark to read the CSV file with the specified schema and returns the resulting DataFrame.
        """
        units_path = os.path.join(self.input_path, "Units_use.csv")
        units_schema = StructType([
            StructField("CRASH_ID", IntegerType(), True),
            StructField("UNIT_NBR", IntegerType(), True),
            StructField("UNIT_DESC_ID", StringType(), True),
            StructField("VEH_PARKED_FL", StringType(), True),
            StructField("VEH_HNR_FL", StringType(), True),
            StructField("VEH_LIC_STATE_ID", StringType(), True),
            StructField("VIN", StringType(), True),
            StructField("VEH_MOD_YEAR", FloatType(), True),
            StructField("VEH_COLOR_ID", StringType(), True),
            StructField("VEH_MAKE_ID", StringType(), True),
            StructField("VEH_MOD_ID", StringType(), True),
            StructField("VEH_BODY_STYL_ID", StringType(), True),
            StructField("EMER_RESPNDR_FL", StringType(), True),
            StructField("OWNR_ZIP", StringType(), True),
            StructField("FIN_RESP_PROOF_ID", StringType(), True),
            StructField("FIN_RESP_TYPE_ID", StringType(), True),
            StructField("VEH_DMAG_AREA_1_ID", StringType(), True),
            StructField("VEH_DMAG_SCL_1_ID", StringType(), True),
            StructField("FORCE_DIR_1_ID", FloatType(), True),
            StructField("VEH_DMAG_AREA_2_ID", StringType(), True),
            StructField("VEH_DMAG_SCL_2_ID", StringType(), True),
            StructField("FORCE_DIR_2_ID", FloatType(), True),
            StructField("VEH_INVENTORIED_FL", StringType(), True),
            StructField("VEH_TRANSP_NAME", StringType(), True),
            StructField("VEH_TRANSP_DEST", StringType(), True),
            StructField("CONTRIB_FACTR_1_ID", StringType(), True),
            StructField("CONTRIB_FACTR_2_ID", StringType(), True),
            StructField("CONTRIB_FACTR_P1_ID", StringType(), True),
            StructField("VEH_TRVL_DIR_ID", StringType(), True),
            StructField("FIRST_HARM_EVT_INV_ID", StringType(), True),
            StructField("INCAP_INJRY_CNT", IntegerType(), True),
            StructField("NONINCAP_INJRY_CNT", IntegerType(), True),
            StructField("POSS_INJRY_CNT", IntegerType(), True),
            StructField("NON_INJRY_CNT", IntegerType(), True),
            StructField("UNKN_INJRY_CNT", IntegerType(), True),
            StructField("TOT_INJRY_CNT", IntegerType(), True),
            StructField("DEATH_CNT", IntegerType(), True)
        ])
        return self.spark.read.format("csv").option("header", "true").schema(units_schema).load(units_path)
