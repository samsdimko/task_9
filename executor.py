from aggregator import PatternAggregator
from data_loader import DataLoader
import threading
import os


class Executor(threading.Thread):
    def __init__(self, directory, archive_directory=None):
        super().__init__()
        self._stop_event = threading.Event()
        self.directory = directory
        self.archive_directory = archive_directory

        self.aggregator = PatternAggregator()
        self.data_loader = DataLoader(*self.aggregator.get_integrated_loader_parameters())

    def execute(self, path):
        for executing_data in self.data_loader.load_data(path):
            self.aggregator.flow(executing_data)

    def run(self):
        while not self._stop_event.is_set():
            for filename in os.listdir(self.directory):
                if filename.endswith('.csv'):
                    try:
                        filepath = os.path.join(self.directory, filename)
                        self.execute(filepath)
                        if self.archive_directory:
                            archive_filepath = os.path.join(self.directory, filename)
                            os.replace(filepath, archive_filepath)
                        else:
                            os.remove(filepath)
                    except Exception as e:
                        print(f"Error processing {filename}: {e}")

    def stop(self):
        self._stop_event.set()
