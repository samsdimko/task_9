import pandas as pd
import data_reading_tools
from config import COLUMN_NAMES


class DataLoader(object):
    """
    A class to load log data
    """

    def __init__(self, use_cols=None, window_size=None, chunk_size=10000):
        """

        :param chunk_size: load chunk size, default 10.000.
        :param use_cols: list of columns to load. If None then all columns are used.
        :param window_size: if is not None the data that will be added at the beginning
                            of next chunk if set.
                            Use pd.Timedelta format
        """
        self.window_size = window_size
        self.use_cols = use_cols
        self.chunk_size = chunk_size

    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        A function to load data from csv_files.
        Args:
        :param file_path: path to file to load.

        yields:
            pandas.DataFrame: A chunk of the log data (with the last 'window_size' rows
                              from the previous chunk concatenated if provided).
        """
        prev_chunk_end = None
        for chunk in pd.read_csv(file_path, names=COLUMN_NAMES, usecols=self.use_cols, chunksize=self.chunk_size,
                                 skiprows=1):
            if self.window_size is not None:
                if prev_chunk_end is not None:
                    combined_data = pd.concat([prev_chunk_end, chunk])
                else:
                    combined_data = chunk.copy()
                prev_chunk_end = combined_data.tail(self.get_tail_size(chunk))
                yield combined_data
            else:
                yield chunk

    def get_tail_size(self, chunk: pd.DataFrame) -> int:
        end_time = pd.to_datetime(chunk['sdk_date'].iloc[-1])
        tail_size = chunk['sdk_date'].loc[pd.to_datetime(chunk['sdk_date']) >
                                          end_time - pd.Timedelta(self.window_size)].shape[0]
        return tail_size


