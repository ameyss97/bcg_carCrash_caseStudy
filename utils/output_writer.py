import os
from utils.config_loader import ConfigLoader


# This Python class `OutputWriter` is responsible for saving DataFrames to different CSV output files
# based on a specified argument.
class OutputWriter:
    def __init__(self, spark):
        self.spark = spark
        self.output_path = os.path.join(
            os.getcwd(), ConfigLoader.load_paths_config()["paths"]["output"])
        self.output_one_path = os.path.join(
            self.output_path, "Output_one.csv")
        self.output_two_path = os.path.join(
            self.output_path, "Output_two.csv")
        self.output_three_path = os.path.join(
            self.output_path, "Output_three.csv")
        self.output_four_path = os.path.join(
            self.output_path, "Output_four.csv")
        self.output_five_path = os.path.join(
            self.output_path, "Output_five.csv")
        self.output_six_path = os.path.join(
            self.output_path, "Output_six.csv")
        self.output_seven_path = os.path.join(
            self.output_path, "Output_seven.csv")
        self.output_eight_path = os.path.join(
            self.output_path, "Output_eight.csv")
        self.output_nine_path = os.path.join(
            self.output_path, "Output_nine.csv")
        self.output_ten_path = os.path.join(
            self.output_path, "Output_ten.csv")

    def save_output(self, dfname, argument):
        """
        This function saves the output DataFrame to a CSV file based on the provided argument number.
        
        :param dfname: It looks like the code snippet you provided is a method for saving the output of
        a DataFrame to a CSV file based on the value of the `argument` parameter. The method takes the
        DataFrame (`dfname`) and an integer argument as input
        :param argument: The `argument` parameter in the `save_output` function seems to be used to
        determine which output path to write the DataFrame to. The function checks the value of
        `argument` and then writes the DataFrame to the corresponding output path based on the value of
        `argument`
        """
        if argument == 1:
            dfname.write.csv(self.output_one_path,
                             mode="overwrite", header=True)
        elif argument == 2:
            dfname.write.csv(self.output_two_path,
                             mode="overwrite", header=True)
        elif argument == 3:
            dfname.write.csv(self.output_three_path,
                             mode="overwrite", header=True)
        elif argument == 4:
            dfname.write.csv(self.output_four_path,
                             mode="overwrite", header=True)
        elif argument == 5:
            dfname.write.csv(self.output_five_path,
                             mode="overwrite", header=True)
        elif argument == 6:
            dfname.write.csv(self.output_six_path,
                             mode="overwrite", header=True)
        elif argument == 7:
            dfname.write.csv(self.output_seven_path,
                             mode="overwrite", header=True)
        elif argument == 8:
            dfname.write.csv(self.output_eight_path,
                             mode="overwrite", header=True)
        elif argument == 9:
            dfname.write.csv(self.output_nine_path,
                             mode="overwrite", header=True)
        elif argument == 10:
            dfname.write.csv(self.output_ten_path,
                             mode="overwrite", header=True)
