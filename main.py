from jobs.analyser import Analyser
from pyspark.sql import SparkSession
from utils.data_loader import DataLoader
from utils.output_writer import OutputWriter


def get_spark_session():
    """
    The function `get_spark_session` creates and returns a Spark session with the app name
    "CarCrashAnalytics".
    :return: The function `get_spark_session()` returns a Spark session with the application name
    "CarCrashAnalytics".
    """
    spark = SparkSession.builder \
        .appName("CarCrashAnalytics") \
        .getOrCreate()
    return spark


def main():
    """
    The `main` function initializes a Spark session, loads data from various sources, performs multiple
    analysis tasks using an `Analyser` object, and saves the output using an `OutputWriter` object
    before stopping the Spark session.
    """

    # Initialize Spark session
    spark = get_spark_session()

    # Load data
    data_loader = DataLoader(spark)
    charges_df = data_loader.load_charges()
    damages_df = data_loader.load_damages()
    endorse_df = data_loader.load_endorse()
    primary_person_df = data_loader.load_primary_person()
    restrict_df = data_loader.load_restrict()
    units_df = data_loader.load_units()

    # Analysis Jobs and save the Output
    analyser = Analyser(spark, charges_df, damages_df,
                        endorse_df, primary_person_df, restrict_df, units_df)

    writer = OutputWriter(spark)

    # Analysis 1
    output_one = analyser.analysis_one()
    writer.save_output(output_one, 1)

    # Analysis 2
    output_two = analyser.analysis_two()
    writer.save_output(output_two, 2)

    # Analysis 3
    output_three = analyser.analysis_three()
    writer.save_output(output_three, 3)

    # Analysis 4
    output_four = analyser.analysis_four()
    writer.save_output(output_four, 4)

    # Analysis 5
    output_five = analyser.analysis_five()
    writer.save_output(output_five, 5)

    # Analysis 6
    output_six = analyser.analysis_six()
    writer.save_output(output_six, 6)

    # Analysis 7
    output_seven = analyser.analysis_seven()
    writer.save_output(output_seven, 7)

    # Analysis 8
    output_eight = analyser.analysis_eight()
    writer.save_output(output_eight, 8)

    # Analysis 9
    output_nine = analyser.analysis_nine()
    writer.save_output(output_nine, 9)

    # Analysis 10
    output_ten = analyser.analysis_ten()
    writer.save_output(output_ten, 10)

    # End Spark session
    spark.stop()


if __name__ == "__main__":
    main()
