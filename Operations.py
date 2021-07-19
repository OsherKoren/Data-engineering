from DB import DB
from Logger import Mylogger
import pandas as pd
import numpy as np
import time
import os
from tools import generate_random_string, log_time, k_merge
from heapq import merge
from concurrent.futures import ProcessPoolExecutor, as_completed


class Operation:
    def __init__(self, source_table='adsTable', result_table=None):
        self.source_table = source_table
        self.result_table = result_table if result_table else source_table
        self.logger = Mylogger('Ads_logger').log()
        self.db = DB(self.logger)

    # Test connection
    def test_connection(self):
        try:
            self.logger.info(' #### ---- Running test connection ---- ####')
            connect = self.db.test_connection()
            if connect:
                self.logger.info(' #### ---- Tested Connection ---- Connected to the database #### ---- ')
        except Exception as e:
            self.logger.error(' #### --- Tested Connection --- Failed to connect to the database #### --- ' + repr(e))
            return False
        return True

    # Setup
    @log_time
    def set_ads_table(self, num_rows=20000):
        rows = 0
        check_table = self.db.check_if_table_exists(self.source_table)
        if check_table:
            rows = self.db.check_number_of_rows(self.source_table)  # Check if the table is empty
        if rows != num_rows:
            np.random.seed(42)
            try:
                ads_df = pd.DataFrame(np.random.randint(0, 10000, size=(num_rows, 1)), columns=['adID'])
                ads_df['Data'] = ads_df.apply(lambda x: generate_random_string(), axis=1)
                self.db.df_to_sql(ads_df, self.source_table)
                return True

            except (ValueError, TypeError) as e:
                self.logger.error('Error in Creating ads table ' + repr(e))
                return False

    # Sort operations
    def sort_operation(self, num, column_to_sort='Data'):
        self.logger.info(f' #### ---- Running sort operation No.{num} ---- #### ')
        if num == 1:
            sorted_column = 'Sorting - step1'
            time_column = 'Sorting-step1_Process_time'
            self.sort_ads(column_to_sort, sorted_column, time_column)
        elif num == 2:
            sorted_column = 'Sorting - step2'
            time_column = 'Sorting-step2_Process_time'
            self.sort_chunks(column_to_sort, sorted_column, time_column)
        elif num == 3:
            sorted_column = 'Sorting - step3'
            time_column = 'Sorting-step3_Process_time'
            self.sort_chunks_parallel(column_to_sort, sorted_column, time_column)
        else:
            self.logger.error(f'Unknown operation number - {num}. Please try again ')
            return False

        self.logger.info(f' #### ---- Finished Running sort operation No.{num} ---- #### ')

    # Sort program - step 1
    @log_time
    def sort_ads(self, column_to_sort, sorted_column, time_column):
        start = time.perf_counter()
        try:
            query = f'SELECT {column_to_sort} FROM {self.source_table}'
            result = self.db.engine.execute(query)
            ads = [row[0] for row in result]
            sorted_ads = sorted(ads)
            sorted_ads = pd.DataFrame(sorted_ads, columns=[sorted_column])

            self.db.df_to_sql(sorted_ads, self.result_table)
            finish = time.perf_counter()
            sorted_ads[time_column] = finish - start
            sorted_ads.to_csv(os.getcwd() + r'\result.csv', index=False)
            self.db.df_to_sql(sorted_ads, self.result_table)

        except (ValueError, TypeError) as e:
            self.logger.error(f' ---- Error in Creating {self.result_table} table ---- ' + repr(e))
            return False

    # Sort program - step 2
    @log_time
    def sort_chunks(self, column_to_sort, sorted_column, time_column, ordered_by_col='adID', chunk=2000):
        start = time.perf_counter()
        try:
            ads_lists = self.query_in_chunks(column_to_sort=column_to_sort, ordered_by_col=ordered_by_col, chunk=chunk)
            if ads_lists:
                sorted_ads = k_merge(*ads_lists)

                try:
                    df = pd.read_csv('result.csv')
                except FileNotFoundError as e:
                    self.logger.error(f'---- Could not find file result.csv in the current directory ----' + repr(e))
                    df = pd.DataFrame()

                df[sorted_column] = sorted_ads
                self.db.df_to_sql(df, self.result_table)

                finish = time.perf_counter()
                df[time_column] = finish - start
                # Create an empty table - just for structure
                df.to_csv(os.getcwd() + r'\result.csv', index=False)

        except ValueError as e:
            self.logger.error(f'Failed to sort {sorted_column} with chunks. ' + repr(e))

    def query_in_chunks(self, column_to_sort, ordered_by_col, chunk):
        ads_lists = []
        offset = 0
        num_of_rows = self.db.check_number_of_rows(self.source_table)

        while offset <= num_of_rows - chunk:
            results = self.db.select_chunks(table_name=self.source_table, column_name=column_to_sort,
                                            ordered_by_col=ordered_by_col, offset=offset, chunk=chunk)
            ads_lists.append(results)
            offset += chunk
            self.logger.info(f'-- Got {offset} values from column {column_to_sort} from table {self.source_table} -- ')

        sorted_lists = [sorted(lst) for lst in ads_lists]

        self.logger.info(f'-- Finished getting all data from column {column_to_sort} from table {self.source_table} --')
        return sorted_lists

    @log_time
    def sort_chunks_parallel(self, column_to_sort, sorted_column, time_column, ordered_by_col='adID', chunk=2000):
        start = time.perf_counter()
        ads_lists = self.query_in_chunks_parallel(column_to_sort=column_to_sort, ordered_by_col=ordered_by_col,
                                                  chunk=chunk)
        if ads_lists:
            sorted_ads = list(merge(*ads_lists))

            try:
                df = pd.read_csv('result.csv')
            except FileNotFoundError as e:
                self.logger.error(f'---- Could not find file result.csv in the current directory ----' + repr(e))
                df = pd.DataFrame()

            df[sorted_column] = sorted_ads
            self.db.df_to_sql(df, self.result_table)
            self.logger.info(f'-- Finished with Multi Processing -- '
                             f'Updated {len(sorted_ads)} values in column Sorting-step3 in table {self.result_table}')
            finish = time.perf_counter()
            df[time_column] = finish - start
            df.to_csv(os.getcwd() + r'\result.csv', index=False)
            self.db.df_to_sql(df, self.result_table)

    def query_in_chunks_parallel(self, column_to_sort, ordered_by_col, chunk):
        ads_lists = []
        futures = []
        offset = 0
        num_of_rows = self.db.check_number_of_rows(self.source_table)
        try:
            with ProcessPoolExecutor(max_workers=4) as executor:
                while offset <= num_of_rows - chunk:
                    futures.append(executor.submit(self.query_a_chunk, column_to_sort, ordered_by_col, offset, chunk))
                    offset += chunk

            for future in as_completed(futures, timeout=60):
                ads_lists.append(future.result())

            self.logger.info(f'-- Got {offset} values from column {column_to_sort} from table {self.result_table} -- ')

            sorted_lists = [sorted(lst) for lst in ads_lists]
            self.logger.info(f'Got all data from column {column_to_sort} from table {self.source_table} ')
            return sorted_lists

        except (KeyboardInterrupt, TypeError) as e:
            self.logger.error(' ---- Error in multi processing ---- ' + repr(e))
            return ads_lists

    def query_a_chunk(self, column_to_sort, ordered_by_col, offset, chunk):
        try:
            return self.db.select_chunks(table_name=self.source_table, column_name=column_to_sort,
                                         ordered_by_col=ordered_by_col, offset=offset, chunk=chunk)
        except AttributeError as e:
            self.logger.error(f'---- Failed to query a chunk ---- from {offset} to {chunk} ---- ' + repr(e))

    def select_results(self):
        results = self.db.select_results()
        print('RESULTS ')
        for row in results:
            print(row)

    # Close connection
    def close_connection(self):
        self.db.engine.dispose()
        self.logger.info(' #### ---- Closing the connection to the database ---- ####')
