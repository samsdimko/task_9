import pandas as pd
from patterns import Pattern
from config_tools import load_config


class PatternAggregator(object):
    """
    A class that orchestrates behaviour of each Pattern subclass.
    """
    __instance = None

    def __new__(cls):
        if not cls.__instance:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def __init__(self):
        # Initialisation of each pattern
        self.patterns = []

        configs = load_config()
        patterns = configs.get('patterns', {})
        if not patterns:
            self.patterns = [cls() for cls in Pattern.__subclasses__()]
        else:
            self.patterns = []
            for cls in Pattern.__subclasses__():
                pattern_args = patterns.get(cls.__name__, {})
                if pattern_args is None:
                    pattern = cls()
                    self.register_pattern(pattern)

                if type(pattern_args) is dict:
                    if pattern_args.get('not_use', False):
                        continue
                    if 'not_use' in pattern_args:
                        del pattern_args['not_use']
                    pattern = cls()
                    for key, value in pattern_args.items():
                        setattr(pattern, key, value)
                    self.register_pattern(pattern)

                elif type(pattern_args) is list:
                    for arg_set in pattern_args:
                        if arg_set.get('not_use', False):
                            continue
                        if 'not_use' in arg_set.keys():
                            del arg_set['not_use']
                        pattern = cls()
                        for key, value in arg_set.items():
                            setattr(pattern, key, value)
                        self.register_pattern(pattern)

    def flow(self, data):
        """
        A method that starts searching and alerting in data for each pattern.
        """
        for pattern in self.patterns:
            pattern.window_flow(data)

    def register_pattern(self, pattern):
        """
        Registers a pattern object with the aggregator.
        Args:
        :param pattern: An object of a Pattern subclass representing the pattern to register.
        """
        self.patterns.append(pattern)

    def get_integrated_loader_parameters(self):
        """
        Method to find values for data loader that will provide optimized way to load data
        based on specific needs of each pattern. Used by DataLoader.
        """
        columns = set()
        window_size = pd.Timedelta(0)
        for pattern in self.patterns:
            columns.update(pattern.get_use_cols())
            window_size = max(window_size, pd.Timedelta(pattern.get_window_size()))

        return list(columns), window_size
