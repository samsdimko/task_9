import pandas as pd
import os
import shutil
from data_loader import DataLoader
from config import COLUMN_NAMES
from data_reading_tools import fix_datetime


class DataPreparer(object):
    def __init__(self, filepath: str, merge_directory='merging/', destination_folder='source', chunk_size=10000):
        self.chunk_size = chunk_size
        self.filepath = filepath
        if len(self.filepath.split('/')) > 1:
            self.filename = self.filepath.split('/')[-1]
        else:
            self.filename = self.filepath
        self.destination_folder = destination_folder
        self.merge_directory = merge_directory
        os.makedirs(self.merge_directory, exist_ok=True)

    @staticmethod
    def prepare_file_for_merge_sort(data: pd.DataFrame) -> pd.DataFrame:
        data = fix_datetime(data)
        data = data.sort_values(by=['sdk_date'])
        return data

    def save_file(self, data: pd.DataFrame, to_end: bool = False, merge_iteration=0, file_number=0):
        filename = f'{self.merge_directory}_{merge_iteration}_{file_number}.csv'
        if not to_end or not os.path.exists(filename):
            data.to_csv(filename, index=False)
        else:
            if os.path.exists(filename):
                data.to_csv(filename, mode='a', header=False, index=False)

    def get_iteration_files(self, iteration):
        filenames = os.listdir(self.merge_directory)
        result_file_list = []
        for filename in filenames:
            try:
                filename = filename.split('_')
                if int(filename[-2]) == iteration:
                    result_file_list.append(filename)
            except:
                continue
        return result_file_list

    def remove_iteration(self, iteration):
        filenames = os.listdir(self.merge_directory)
        for filename in filenames:
            try:
                split_filename = filename.split('_')
                if int(split_filename[-2]) == iteration:
                    os.remove(f'{self.merge_directory}{filename}')
            except:
                continue

    def update_merging_chunk(self, chunk: pd.DataFrame, loader, str_index):
        sub_chunk = next(loader)
        sub_chunk['original_df'] = str_index
        sub_chunk.set_index(['original_df'], inplace=True, append=True)
        if len(chunk) > 0:
            chunk = pd.concat([chunk, sub_chunk])
            return chunk
        return sub_chunk

    @staticmethod
    def delete_saved_rows_in_chunk(chunk: pd.DataFrame, working_chunk: pd.DataFrame):
        deleting_indexes = chunk.index.isin(working_chunk.index)
        chunk.drop(chunk[~deleting_indexes].index, inplace=True)

    def merge_files(self, iteration, first_id, second_id=None):
        if second_id is None:
            shutil.copy2(f'{self.merge_directory}_{iteration}_{first_id}.csv',
                         f'{self.merge_directory}_{iteration + 1}_{first_id // 2}.csv')
            return
        first_loader = DataLoader(use_cols=COLUMN_NAMES, chunk_size=self.chunk_size)
        first_loader = first_loader.load_data(f'{self.merge_directory}_{iteration}_{first_id}.csv')
        second_loader = DataLoader(use_cols=COLUMN_NAMES, chunk_size=self.chunk_size)
        second_loader = second_loader.load_data(f'{self.merge_directory}_{iteration}_{second_id}.csv')

        loaders_emptiness = [False, False]
        chunk_1 = pd.DataFrame(columns=COLUMN_NAMES + ['original_df', 'index'])
        chunk_2 = pd.DataFrame(columns=COLUMN_NAMES + ['original_df', 'index'])
        while not any(loaders_emptiness):
            try:
                if len(chunk_1) < 2 * self.chunk_size:
                    chunk_1 = self.update_merging_chunk(chunk_1, first_loader, 'first')
            except StopIteration:
                if len(chunk_1) == 0:
                    loaders_emptiness[0] = True

            try:
                if len(chunk_2) < 2 * self.chunk_size:
                    chunk_2 = self.update_merging_chunk(chunk_2, second_loader, 'second')
            except StopIteration:
                if len(chunk_2) == 0:
                    loaders_emptiness[1] = True

            chucks_to_merge = [chunk_1, chunk_2]
            chucks_to_merge = [chunk for chunk in chucks_to_merge if len(chunk) > 0]
            if len(chucks_to_merge) > 0:
                working_chunk = pd.concat(chucks_to_merge)
                working_chunk = working_chunk.sort_values(by=['sdk_date'])
                df_to_save = working_chunk.iloc[:self.chunk_size]
                working_chunk = working_chunk.iloc[self.chunk_size:]
                self.delete_saved_rows_in_chunk(chunk_1, working_chunk)
                self.delete_saved_rows_in_chunk(chunk_2, working_chunk)

                self.save_file(df_to_save, True, iteration + 1, first_id // 2)

    def proceed_merge(self, iteration=0):
        self.remove_iteration(iteration+1)
        iteration_filenames = self.get_iteration_files(iteration)
        file_number = len(iteration_filenames)
        last_iteration = iteration
        if file_number == 1:
            return last_iteration
        for i in range(0, file_number, 2):
            if i != file_number - 1:
                self.merge_files(iteration, i, i + 1)
            else:
                self.merge_files(iteration, i)
        last_iteration = self.proceed_merge(iteration + 1)
        return last_iteration

    def prepare_merge(self):
        loader = DataLoader(use_cols=COLUMN_NAMES, chunk_size=self.chunk_size)
        for i, chunk in enumerate(loader.load_data(self.filepath)):
            chunk = self.prepare_file_for_merge_sort(chunk)
            self.save_file(chunk, file_number=i)

    def new_source(self, iteration):
        new_filename = f'_{iteration}_0.csv'
        os.replace(f'merging/{new_filename}', f'{self.destination_folder}/{self.filename}')

    def merge_sort(self):
        self.prepare_merge()
        print('Files are created!')
        new_file_iteration = self.proceed_merge()
        print('Merge is finished!')
        self.new_source(new_file_iteration)
        print('New source is ready!')
