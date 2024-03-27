from abc import ABC, abstractmethod
from typing import Union, List, Tuple
import pandas as pd
from data_reading_tools import convert_datetime


class Pattern(ABC):
    def __init__(self, window_size: str = '1 minute', message: str = '',
                 use_cols: Union[List[str], Tuple[str]] = ('sdk_date',)):
        self.window_size = window_size
        self.message = message
        self.use_cols = use_cols

    def window_flow(self, data: pd.DataFrame):
        """
        A method that implements flowing and processing window through the data.
        """
        data['sdk_date'] = data['sdk_date'].apply(convert_datetime)
        data = self.match_columns(data)
        work_data = data.set_index('sdk_date')['error_code']
        alert_data = work_data.rolling(window=pd.Timedelta(self.window_size))
        alert_data.apply(self.match_decorator())

    def alert(self, info):
        print(self.message + info)

    def get_window_size(self):
        return self.window_size

    def get_use_cols(self):
        return self.use_cols

    def match_columns(self, data):
        """
        If you use some conditions based on non-numeric values
        you can implement your custom selection logic here by
        inheritance
        """
        if len(self.use_cols) > 2:
            for col in self.use_cols:
                if col not in {'sdk_date', 'error_code'}:
                    data = data.loc[data[col] == getattr(self, col)]
        return data

    @abstractmethod
    def match(self, data):
        """
        Abstract method to be reimplemented for searching patterns in child classes
        Should call a self. alert method to alert
        """
        pass

    def match_decorator(self):
        """
        Decorator to implement rolling without returning a value
        """
        def dec(*args, **kwargs):
            details = self.match(*args, **kwargs)
            if details:
                self.alert(details)
            return 0
        return dec


class EachMinute10FatalErrorsPattern(Pattern):
    def match(self, data):
        errors = data.loc[data == 0]
        if errors.shape[0] > 10:
            return f'Event period: {errors.index[0]} - {errors.index[-1]}'


class EachHour10FatalErrorsForSpecificBundleIdPattern(Pattern):
    def match(self, data):
        errors = data.loc[data == 0]
        if errors.shape[0] > 10:
            return f'Event period: {errors.index[0]} - {errors.index[-1]}, bundle_id: {self.bundle_id}'
