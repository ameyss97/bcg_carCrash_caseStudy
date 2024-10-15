import os
import yaml


# The `ConfigLoader` class in Python contains a static method `load_paths_config` that loads a YAML
# configuration file containing paths.
class ConfigLoader:
    @staticmethod
    def load_paths_config():
        """
        The function `load_paths_config` reads and loads a YAML file containing path configurations.
        :return: The function `load_paths_config` is returning the contents of the 'path_config.yaml'
        file located in the 'config' directory within the current working directory. The contents are
        loaded using the `yaml.safe_load` function and returned as a Python object.
        """
        file_path = os.path.join(os.getcwd(), 'config', 'path_config.yaml')
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
