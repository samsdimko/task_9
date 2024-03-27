from executor import Executor
from data_preparer import DataPreparer
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


def main(work_directory='source', prepare_directory='raw_data'):
    # data_preparer = DataPreparer(f'{prepare_directory}/data.csv', destination_folder=work_directory)
    # data_preparer.merge_sort()
    executor = Executor(work_directory)
    executor.start()


if __name__ == '__main__':
    main()
