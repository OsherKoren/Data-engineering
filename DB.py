from Logger import Mylogger
from Conf import Conf
from sqlalchemy import create_engine, text
import urllib


class DB:
    def __init__(self, logger=None):
        self.logger = logger if logger else Mylogger('Ads_logger').log()
        self.conf = Conf(self.logger, local=False)

        try:
            db_config = self.conf.get_db()
            driver = db_config['driver']
            host = db_config['host']
            self.db = db_config['db']
            user = db_config['user']
            password = db_config['password']
            connection = db_config['connection']

            if connection == 'Remote':
                conn_str = 'DRIVER=' + driver + ';' + \
                           'SERVER=' + host + ';' + \
                           'DATABASE=' + self.db + ';' + \
                           'UID=' + user + ';' + \
                           'PWD=' + password + ';'
                self.logger.info(f' ---- Connection to {self.db} Database is set with remote server ---- ')

            else:  # Local configuration - Relevant parameters should by set in local_db_config.ini file
                conn_str = 'DRIVER=' + driver + ';' + \
                           'SERVER=' + host + ';' + \
                           'DATABASE=' + self.db + ';' + \
                            'TRUSTED_CONNECTION=YES;'
                self.logger.info(f' ---- Connection to {self.db} Database is set with local server ---- ')

            quoted = urllib.parse.quote_plus(conn_str)
# self.conn_str For later use/access when dealing with multiprocessing with the exclusion of engine from pickling
            self.conn_str = conn_str
            self.engine = create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')
            self.engine.execution_options(stream_results=True)
            self.logger.info(f' ---- Database session started. Connecting to {self.db} Database ---- ')

        except (AttributeError, KeyError) as e:
            self.logger.error('Error in DB Configuration. Cannot Connect to DB. '
                              'Please make sure that the remote server is running ' + repr(e))

# Two methods __getstate__ and __setstate__ to deal with unpickling self.engine when multiprocessing
    # This method is called when you are going to pickle the class, to know what to pickle
    def __getstate__(self):
        state = self.__dict__.copy()
        # don't pickle the self.engine
        del state['engine']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # retrieve the excluded method/methods
        self.engine = create_engine(f'mssql+pyodbc:///?odbc_connect={self.conn_str}')

    def test_connection(self):
        conn = self.engine.connect()
        if conn:
            return True
        else:
            return False

    def check_if_table_exists(self, table_name):
        try:
            query = f"SELECT TOP 1 * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table_name}'"
            result = self.engine.execute(text(query)).fetchone()
            self.logger.info(f' ---- The table {table_name} exists in database {self.db} ---- ')

        except AttributeError as e:
            self.logger.info(f' ---- The table {table_name} does not exists in database {self.db} ---- ' + repr(e))
            return False
        return result

    def check_number_of_rows(self, table_name):
        try:
            query = text(f'SELECT COUNT(*) FROM {table_name}')
            result = self.engine.execute(query)
            num_of_rows = result.fetchone()
            self.logger.info(f' ---- The table {table_name} exists with {num_of_rows[0]} rows ---- ')
            if num_of_rows:
                return num_of_rows[0]

        except AttributeError as e:
            self.logger.info(f' ---- The table {table_name} has no data ---- ' + repr(e))
            return False

    def df_to_sql(self, df, table_name, chunksize=100, method='multi'):
        df.to_sql(name=table_name, con=self.engine, if_exists='replace', chunksize=chunksize, method=method)
        self.logger.info(f' ---- Stored data into table named: {table_name} in the database: {self.db} ---- ')

    def select_chunks(self, table_name, column_name, ordered_by_col, offset, chunk):
        try:
            if offset == 0:
                query = f'SELECT TOP {chunk} [{column_name}] ' \
                        f'FROM [{table_name}] '
            else:
                query = f'SELECT [{column_name}] ' \
                        f'FROM {table_name} ' \
                        f'ORDER BY {ordered_by_col} ' \
                        f'OFFSET {offset} ROWS ' \
                        f'FETCH NEXT {chunk} ROWS ONLY'

            results = [item[0] for item in self.engine.execute(text(query)).fetchall()]
            return results
        except Exception as e:
            self.logger.error(f'---- Stopped querying in chunks ----' + repr(e))
            return False

    def select_results(self):
        query = 'SELECT TOP 10 * FROM RESULTS'
        results = self.engine.execute(text(query)).fetchall()
        return results
