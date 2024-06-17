from abc import ABC, abstractmethod
import pyodbc
import pandas as pd
import os
from tqdm import tqdm

import logging

logging.getLogger().setLevel(logging.INFO)

class Connector():
    """
    Class handling loading/exporting data.
    Loading is suported from: - csv  use
                              - excel
                              - json
                              - database
    Exporting is supported to: - csv
                               - excel
                               - json
                               - database
    :param: load_from (str): Can be one of `csv`, `excel`, `json`, `db`, `auto`
    :param: export_to (str): Can be one of `csv`, `excel`, `db`, `auto`
    :param: in_file_path
    :param: out_file_path
    :param: server
    :param: db
    :param: in_table
    :param: out_table
    :param: schema (str): Schema of `out_table`. It can be specified or infered (`schema='auto'`)
    :param: mode (str): How to insert data. If `append` then data will be appended to existing file/table.
                                            If `write` new file/table will be created.
                                            Note that if `mode='append'` and `export_to='db'` we expect that output DataFrame has
                                            schema corresponds to `out_table` schema
    """
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        
    def _infer_from(self, load_from: str = 'auto', in_file_path: str = '', server: str = '',
                     db: str = '', in_table: str = '') -> str:
        """
        auxiliary method to automaticaly infer `load_from` argument.
        Loading from database is prefered i.e. if both `in_file_path` and `in_table` is specified
        loading from `in_table` will be choosen.

        Parameters
        load_from: str
            Specify from which source load. Can be one of `csv`, `excel`, `db`, `auto`.
            Default is `auto` which means that type of input source will be infered from other parameters
        in_file_path: str
            Absolute path of input file to be loaded
        server: str
            server name (server in local network)
        db: str
            Name of sql server database
        in_table: str
            Optional name of input sql server table
        """
        if load_from == 'auto':
            if not in_table and in_file_path:
                if os.path.splitext(in_file_path)[-1] == '.csv':
                    return 'csv'
                elif os.path.splitext(in_file_path)[-1] == '.json':
                    return 'json'
                elif os.path.splitext(in_file_path)[-1] == '.xlsx':
                    return 'excel'

                else:
                    raise ValueError('Unable to infer input source type. Please specify `load_from` parameter or'
                                     'make sure `in_file_path` has correct extension (.csv or .xlsx).')
            elif not in_table and not in_file_path:
                 raise ValueError('Neither `in_table` nor `in_file_path` specified. Unable to load data.')
            elif in_table: 
                if not server or not db:
                    raise ValueError('Both `server` and `db` parameters must be specified to load from database.')
                return 'db'
                
            else:
                raise ValueError('Unable to infer input source type. Please specify `load_from` parameter')
            
        if load_from not in ['csv', 'excel', 'json', 'db']:
            raise ValueError(f'Invalid value for load_from: {load_from}')

        if load_from != 'db':
            expected_extension = '.' + {'excel': 'xlsx', 'csv': 'csv', 'json': 'json'}[load_from]
            actual_extension = os.path.splitext(in_file_path)[-1]
            if expected_extension != actual_extension:
                raise ValueError(f'`load_from={load_from}` does not correspond to input file extension {actual_extension}')

        return load_from
            
            
    def _infer_to(self, export_to: str = 'auto', out_file_path: str = '', server: str = '',
                     db: str = '', out_table: str = '') -> str:
        """
        auxiliary method to automaticaly infer `export_to` argument
        Exporting to database is prefered i.e. if both `out_file_path` and `out_table` are specified
        exporting to `out_table` will be choosen.
        Parameters
        export_to: str
            Specify destination type of data. Can be one of `csv`, `excel`, `db`, `auto`.
            Default is `auto` which means that type of output destination will be infered from other parameters
        out_file_path: str
            Absolute path of output file 
        server: str
            server name (server in local network)
        db: str
            Name of sql server database
        out_table: str
            Optional name of output sql server table
        """
        if export_to == 'auto':
            if not out_table and not out_file_path:
                raise ValueError('Neither `out_table` nor `out_file_path` specified. Unable to export data.')
        
            if not out_table and out_file_path:
                file_extension = os.path.splitext(out_file_path)[-1]
                if file_extension == '.csv':
                    return 'csv'
                elif file_extension == '.json':
                    return 'json'
                elif file_extension == '.xlsx':
                    return 'excel'
                else:
                    raise ValueError('Unable to infer output source type. Unsupported file extension or missing `export_to` specification.')

            if out_table:
                if not server or not db:
                    raise ValueError('Both `server` and `db` parameters must be specified to export to a database.')
                return 'db'
        elif export_to not in ['csv', 'excel', 'json', 'db']:
            raise ValueError(f'Invalid value for export_to: {export_to}. Valid options are `csv`, `excel`, `json`, `db`, or `auto`.')
        
        return export_to
        
    def _load_from_db(self, server: str = '', db: str = '', in_table: str = '', *args, **kwargs) -> pd.DataFrame:
        """
        auxiliary method to load data from sql server database

        Parameters
        server: str
            server name (server in local network)
        db: str
            Name of sql server database
        in_table: str
            Name of input sql server table
        """
        possible_drivers = ['SQL Server Native Client 11.0', 'ODBC Driver 17 for SQL Server'] 
        installed_drivers = pyodbc.drivers()
        drivers = [i for i in possible_drivers if i in installed_drivers]
        
        if not drivers:
            error_message = "No suitable SQL Server drivers installed."
            logging.error(error_message)
            raise EnvironmentError(error_message)

        driver = '{' + drivers[0] + '}'
        logging.info(f'Loading from databae: {server}.{db}.dbo.{in_table}')

        try:
            connection_string = f'Driver={driver};Server={server};Database={db};Trusted_Connection=yes;'
            with pyodbc.connect(connection_string, autocommit=True) as conn:
                in_data = pd.read_sql_query(f'SELECT * FROM {in_table}', conn, *args, **kwargs)

        except pyodbc.Error as e:
            logging.error(f"Database connection failed: {e}")
            raise ConnectionError(f"Database connection failed: {e}")
        except pd.errors.DatabaseError as e:
            logging.error(f"SQL query execution failed: {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            page = 'https://learn.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?source=recommendations&view=sql-server-ver16'
            logging.info('Probably there is not installed proper driver for SQL server on this machine. To load data from database please follow \n'
                  f'instructions on this page {page} \n.'
                  'You can also consider loading from excel/csv file if needed.')
            raise RuntimeError(f"An unexpected error occurred: {e}")

        return in_data
    
    def _load_from_file(self, file_type, in_file_path, *args, **kwargs) -> pd.DataFrame:
        """
        Generic method to load data from a specified file type.

        Parameters:
        file_type: str
            Type of the file to load ('excel', 'csv', 'json')
        in_file_path: str
            Absolute path of the file to be loaded
        """
        logging.info(f'Loading from {file_type}: {in_file_path}')
        load_function = {
            'excel': pd.read_excel,
            'csv': pd.read_csv,
            'json': pd.read_json
        }.get(file_type)

        if not load_function:
            raise ValueError(f"Unsupported file type: {file_type}")

        try:
            return load_function(in_file_path, *args, **kwargs)
        except FileNotFoundError:
            error_msg = f'The file {in_file_path} was not found.'
            logging.error(error_msg)
            raise FileNotFoundError(error_msg)
        except ValueError as ve:
            error_msg = f'Value error while loading the {file_type} file: {ve}'
            logging.error(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f'An unexpected error occurred while loading the {file_type} file: {e}'
            logging.error(error_msg)
            raise RuntimeError(error_msg)

    def load(self, load_from: str = 'auto', in_file_path: str = '',
             server: str = '', db: str = '', in_table: str = '', *args, **kwargs) -> pd.DataFrame:
        """
        Method to load data from csv, json, excel files, or a SQL server database.

        Parameters:
        load_from : str
            Specify the source type from which to load. Options are 'csv', 'excel', 'db', 'json', 'auto'.
            Default is 'auto', which infers the type from other parameters.
        in_file_path : str
            Absolute path of the input file to be loaded.
        server : str
            Server name (server in the local network).
        db : str
            Name of the SQL server database.
        in_table : str
            Optional name of the input SQL server table.

        Returns:
        pd.DataFrame
            A DataFrame containing the loaded data.

        Raises:
        ValueError
            If the `load_from` parameter is not one of the expected options or if the loading process fails due to unsupported configuration.

        Examples:
        Loading with specific encoding from CSV:
            load('csv', in_file_path='in_file_path.csv', encoding='utf-8')

        Loading a specific sheet from an Excel file:
            load('excel', in_file_path='in_file_path.xlsx', sheet_name='Sheet2')

        Note:
        For specifying additional parameters, please refer to the pandas documentation:
        - For Excel: https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html
        - For CSV: https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
        - For JSON: https://pandas.pydata.org/docs/reference/api/pandas.read_json.html
        - For database: https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html
        """
        
        valid_sources = ['csv', 'json', 'excel', 'db', 'auto']
        if load_from not in valid_sources:
            raise ValueError(f'Argument `load_from` must be one of {valid_sources}, got {load_from}')
        
        load_from = self._infer_from(load_from, in_file_path, server, db, in_table)   

        if load_from == 'csv':
            in_data = self._load_from_file('csv', in_file_path, *args, **kwargs)
        elif load_from == 'json':
            in_data = self._load_from_file('json', in_file_path, *args, **kwargs)
        elif load_from == 'excel':
            in_data = self._load_from_file('excel', in_file_path, *args, **kwargs)
        elif load_from == 'db':
            in_data = self._load_from_db(server, db, in_table, *args, **kwargs)
        else:
            raise ValueError(f'Unsupported loading source `{load_from}`')
        
        return in_data

    def _export_to_db(self, out_data: pd.DataFrame, server: str = '', db: str = '', out_table: str = '',
                      schema: str = 'auto',
                      mode: str='write', *args, **kwargs) -> None:
        """
        auxiliary method for exporting data to sql server database

        Parameters
        out_data: pd.DataFrame
            data to be exported presented as pd.DataFrame
        server: str
            server name (server in local network)
        db: str
            Name of sql server database
        out_table: str
            Name of output sql server table
        schema: str
            Schema of output table.
            Default is `auto` which means that schema will be infered automatically from `out_data` dataframe
        mode: str
            Specify type of export. Can be one of `append` or `write`.
            `append` will append data to existing table
            `write` will first drop existing table and then create new `out_table`

            Default is `write`
        """
        possible_drivers = ['SQL Server Native Client 11.0', 'ODBC Driver 17 for SQL Server'] 
        installed_drivers = pyodbc.drivers()
        drivers = [i for i in possible_drivers if i in installed_drivers]
        driver = '{' + drivers[0] + '}'

        with pyodbc.connect(f'Driver={driver};Server={server};Database={db};Trusted_Connection=yes;', autocommit=True) as conn:
            with conn.cursor() as cursor:
        
                if mode == 'write':
                    self._create_table(schema, out_data, out_table, cursor)

                cursor.fast_executemany = True

                data = out_data.values.tolist()

                quest = len(out_data.columns) * ['?']
                insert_query = f'INSERT INTO {out_table} VALUES ({", ".join(quest)})'

                logging.info(f'Inserting to database: {server}.{db}.dbo.{out_table}...\n'
                    'Please be patient, it can take a while')
                cursor.executemany(insert_query, data)
                logging.info('Data inserted successfuly!')
                
                cursor.commit()
            
    def _export_to_excel(self, out_data: pd.DataFrame, out_file_path: str = '', mode: str='write',
                          *args, **kwargs) -> None:
        """
        auxiliary method for exporting data to excel

        Parameters
        out_data: pd.DataFrame
            data to be exported presented as pd.DataFrame
        out_file_path: str
            Absolute path of output file 
        mode: str
            Specify type of export. Can be one of `append` or `write`.
            `append` will append data to existing table
            `write` will first drop existing table and then create new `out_table`

            Default is `write`
        """
        logging.info(f'Inserting to excel: {out_file_path}')
        mode_flag = 'a' if mode == 'append' else 'w'
        kwargs.setdefault('index', False)

        kwgs = {"if_sheet_exists": 'overlay'} if mode == 'append' else {}

        try:
            with pd.ExcelWriter(out_file_path, mode=mode_flag, engine='openpyxl', **kwgs) as writer:
                out_data.to_excel(writer, sheet_name='output', startrow=writer.sheets['output'].max_row if mode == 'append' else 0, header=True if mode == 'write' else None, **kwargs)
        except Exception as e:
            logging.error(f"Failed to export to Excel: {e}")
            raise
        

    def _export_to_json(self, out_data: pd.DataFrame, out_file_path: str = '', mode: str='write', *args, **kwargs) -> None:
        """
        auxiliary method for exporting data to json
        Not implemented yet

        Parameters
        out_data: pd.DataFrame
            data to be exported presented as pd.DataFrame
        out_file_path: str
            Absolute path of output file 
        mode: str
            Specify type of export. Can be one of `append` or `write`.
            `append` will append data to existing table
            `write` will first drop existing table and then create new `out_table`

            Default is `write`
        """
        raise NotImplementedError

    def _export_to_csv(self, out_data: pd.DataFrame, out_file_path: str = '',  mode: str='write',
                       *args, **kwargs) -> None:
        """
        auxiliary method for exporting data to csv

        Parameters
        out_data: pd.DataFrame
            data to be exported presented as pd.DataFrame
        out_file_path: str
            Absolute path of output file 
        mode: str
            Specify type of export. Can be one of `append` or `write`.
            `append` will append data to existing table
            `write` will first drop existing table and then create new `out_table`

            Default is `write`
        """
        logging.info(f'Exporting to CSV: {out_file_path}')

        mode_flag = 'a' if mode == 'append' else 'w'
        kwargs.setdefault('index', False)
        kwargs.setdefault('mode', mode_flag)
        kwargs.setdefault('header', False if mode == 'append' and os.path.exists(out_file_path) else True)
        kwargs.setdefault('encoding', 'utf-8-sig')

        try:
            out_data.to_csv(out_file_path, *args, **kwargs)
        except Exception as e:
            logging.error(f"Failed to export to CSV: {e}")
            raise

    def export(self, out_data: pd.DataFrame, export_to: str = 'auto', out_file_path: str = '',
                server: str = '', db: str = '', out_table: str = '',
                schema: str = 'auto',
                mode: str='write', *args, **kwargs) -> None:
        """
        Method for exporting data to csv, excel, or SQL server database.

        Parameters:
        out_data : pd.DataFrame
            Data to be exported.
        export_to : str
            Specify destination type of data. Options are 'csv', 'excel', 'db', 'auto'.
        out_file_path : str
            Absolute path of the output file.
        server, db, out_table : str
            Database server, database name, and table name for database exports.
        schema : str
            Schema of the output table, automatically inferred if set to 'auto'.
        mode : str
            'append' to add to existing data, 'write' to overwrite.

        Raises:
        ValueError: If parameters are invalid or missing.
        Exception: For generic errors and non-implemented features.
        """

        if not isinstance(out_data, pd.DataFrame):
            raise ValueError('`out_data` must be of type `pd.DataFrame`')

        if mode not in ['append', 'write']:
            raise ValueError('Mode must be one of `append`, `write`')

        if export_to not in ['csv', 'excel', 'db', 'auto']:
            raise ValueError('Argument `export_to` must be one of `csv`, `excel`, `db`, `auto`')

        export_to = self._infer_to(export_to, out_file_path, server, db, out_table)

        mapping = {'excel': 'xlsx', 'csv': 'csv', 'json': 'json'}

        if export_to != 'db':
            extension = os.path.splitext(out_file_path)[1]
            if not extension:
                out_file_path += f'.{mapping[export_to]}'

        try:
            if export_to == 'csv':
                self._export_to_csv(out_data, out_file_path, mode, *args, **kwargs)
            elif export_to == 'json':
                self._export_to_json(out_data, out_file_path, mode, *args, **kwargs)
            elif export_to == 'excel':
                self._export_to_excel(out_data, out_file_path, mode, *args, **kwargs)
            elif export_to == 'db':
                    self._export_to_db(out_data, server, db, out_table, schema, mode, *args, **kwargs)
        except Exception as e:
            logging.error(f"Error exporting data: {e}")
            raise
        
    def _create_table(self, schema: str, in_data: pd.DataFrame, out_table: str, cursor: pyodbc.Cursor)-> None:
        """
            Create or replace an SQL table based on DataFrame schema.

            Parameters:
            schema : str
                Schema specification, auto-generated if 'auto'.
            in_data : pd.DataFrame
                DataFrame to infer the schema from if needed.
            out_table : str
                Name of the output SQL server table.
            cursor : pyodbc.Cursor
                Active database cursor.
        """

        if schema == 'auto':
            # Inference should handle SQL data types appropriately
            dtypes = {dtype: sql_type for dtype, sql_type in zip(in_data.dtypes, ['VARCHAR(MAX)' if dtype.name == 'object' else 'FLOAT' if dtype.name.startswith('float') else 'INT' for dtype in in_data.dtypes])}
            schema = ', '.join([f"{col} {dtypes[col.dtype]}" for col in in_data])
        
        cursor.execute(f"IF OBJECT_ID('{out_table}') IS NOT NULL DROP TABLE {out_table};")
        cursor.execute(f"CREATE TABLE {out_table} ({schema});")
        logging.info(f"Table {out_table} created successfully.")

if __name__ == '__main__':

    df = pd.DataFrame({'a': [1, 2, 4, 5], 'b': ['a', 'b', 'c', 'e']})

    csv = r'test.csv'
    excel = r'excel.xlsx'
    c = Connector() 

    c.export(df, export_to='csv', out_file_path=csv)
    print(c.load(load_from='auto', in_file_path=csv, encoding='latin-1'))
    c.export(df, export_to='auto', out_file_path=excel)
    print(c.load(load_from='auto', in_file_path=excel))
    c.export(df, export_to='auto', out_file_path=excel)
    print(c.load(load_from='auto', in_file_path=excel))
    c.export(df, export_to='auto', out_file_path=excel, mode='append')
    print(c.load(load_from='auto', in_file_path=excel))
    c.export(df, export_to='csv', out_file_path=r'test')
    print(c.load(load_from='auto', in_file_path=r'test', encoding='latin-1'))