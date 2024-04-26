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
                    raise Exception('Unable to infer input source type. Please specify `load_from` parameter or'
                                    'make sure `in_file_path` has correct extension (.csv or .xlsx).')
            elif not in_table and not in_file_path:
                 raise Exception('`in_table` nor `in_file_path` specified. Unable to load data.')
            elif in_table: 
                if server and db:
                    return 'db'
                else:
                    raise Exception('`server` or `db` parameter is not specified')
            else:
                raise Exception('Unable to infer input source type. Please specify `load_from` parameter')
        
        mapping = {'excel': 'xlsx', 'csv': 'csv', 'json': 'json'}

        if load_from != 'db':
            assert mapping[load_from] in os.path.splitext(in_file_path)[-1], f'`load_from={load_from}` does not correspond to input file extension {os.path.splitext(in_file_path)[-1]}'

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
            if not out_table and out_file_path:
                if os.path.splitext(out_file_path)[-1] == '.csv':
                    return 'csv'
                elif os.path.splitext(out_file_path)[-1] == '.json':
                    return 'json'
                elif os.path.splitext(out_file_path)[-1] == '.xlsx':
                    return 'excel'

                else:
                    raise Exception('Unable to infer output source type. Please specify `export_to` parameter or make sure `out_file_path` has correct extension.')
            elif not out_table and not out_file_path:
                raise Exception('`out_table` nor `out_file_path` specified. Unable to export data.')
            elif out_table:
                if server and db:
                    return 'db'
                else:
                    raise Exception('`server` or `db` parameter is not specified')
            else:
                raise Exception('Unable to infer output source type. Please specify `export_to` parameter')
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
        driver = '{' + drivers[0] + '}'

        logging.info(f'Loading from databae: {server}.{db}.dbo.{in_table}')
        try:
            with pyodbc.connect(f'Driver={driver};Server={server};Database={db};Trusted_Connection=yes;', autocommit=True) as conn:
                in_data = pd.read_sql_query(f'SELECT * FROM {in_table}', conn, *args, **kwargs)
        except Exception as e:
            logging.info(e)
            page = 'https://learn.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?source=recommendations&view=sql-server-ver16'
            logging.info('Probably there is not installed proper driver for SQL server on this machine. To load data from database please follow \n'
                  f'instructions on this page {page} \n.'
                  'You can also consider loading from excel/csv file if needed.')
            raise

        return in_data

    def _load_from_excel(self, in_file_path: str = '', *args, **kwargs) -> pd.DataFrame:
        """
        auxiliary method to load data from excel file

        Parameters
        in_file_path: str
            Absolute path of input file to be loaded
        """
        logging.info(f'Loading from excel: {in_file_path}')
        try:
            return pd.read_excel(in_file_path)
        except Exception as e:
            logging.info(e)
            raise

    def _load_from_csv(self, in_file_path: str = '', *args, **kwargs) -> pd.DataFrame:
        """
        auxiliary method to load data from csv file

        Parameters
        in_file_path: str
            Absolute path of input file to be loaded
        """
        logging.info(f'Loading from csv: {in_file_path}')
        try:
            return pd.read_csv(in_file_path, *args, **kwargs)
        except Exception as e:
            logging.info(e)
            raise

    def _load_from_json(self, in_file_path: str = '', *args, **kwargs) -> pd.DataFrame:
        """
        auxiliary method to load data from json file
        Parameters
        in_file_path: str
            Absolute path of input file to be loaded
        """
        logging.info(f'Loading from json: {in_file_path}')
        try:
            return pd.read_json(in_file_path, *args, **kwargs)
        except Exception as e:
            logging.info(e)
            raise

    def load(self, load_from: str = 'auto', in_file_path: str = '',
             server: str = '', db: str = '', in_table: str = '', *args, **kwargs) -> pd.DataFrame:
        """
        method to load data from csv, json, excel and SQL server database

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

        Note
        For specifying additional parameters please refer to
        https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html .. for excel
        https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html ... for csv
        https://pandas.pydata.org/docs/reference/api/pandas.read_json.html  ... for json
        https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html ... for database

        For instance one may need to specify encoding when loading from csv
        then one can use e.g. `load('in_file_path.csv', encoding='utf-8')`
        which will load data from `in_file_path.csv` and use `encoding='utf-8'`

        or for example load specific sheet from excel:
        `load('in_file_path.xlsx', sheet_name='Sheet2')`
        """
        
        assert load_from in ['csv', 'json', 'excel', 'db', 'auto'], f'Argument `load_from` must be one of' \
                                                                   '`csv`, `json`, `excel`, `db`, `auto`'
        
        load_from = self._infer_from(load_from, in_file_path, server, db, in_table)   

        if load_from == 'csv':
            in_data = self._load_from_csv(in_file_path, *args, **kwargs)
        elif load_from == 'json':
            in_data = self._load_from_json(in_file_path, *args, **kwargs)
        elif load_from == 'excel':
            in_data = self._load_from_excel(in_file_path, *args, **kwargs)
        elif load_from == 'db':
            in_data = self._load_from_db(server, db, in_table, *args, **kwargs)
        else:
            raise Exception(f'Non-implemented for `{load_from}`')
        
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
        m = 'a' if mode == 'append' else 'w'
        kwargs['index'] = False if 'index' not in kwargs else kwargs['index']
        
        try:
            if m == 'a' and os.path.isfile(out_file_path):
                kwargs['header'] = None if 'header' not in kwargs else kwargs['header']
                with pd.ExcelWriter(out_file_path, mode=m, engine='openpyxl',if_sheet_exists='overlay') as writer:
                    out_data.to_excel(writer, sheet_name='output', startrow=writer.sheets['output'].max_row,
                                             *args, **kwargs)
            else:
                kwargs['header'] = True if 'header' not in kwargs else kwargs['header']
                with pd.ExcelWriter(out_file_path, mode=m) as writer:
                    out_data.to_excel(writer, sheet_name='output', *args, **kwargs)
        except Exception as e:
            logging.info(e)
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
        m = 'a' if mode == 'append' else 'w'
        kwargs['index'] = False if 'index' not in kwargs else kwargs['index']
        kwargs['mode'] = m if 'mode' not in kwargs else kwargs['mode']
        kwargs['header'] = not os.path.isfile(out_file_path) or kwargs['mode']=='w' if 'header' not in kwargs else kwargs['header']
        kwargs['encoding'] = 'utf-8-sig' if 'encoding' not in kwargs else kwargs['encoding']

        logging.info(f'Inserting to csv: {out_file_path}')

        try:
            out_data.to_csv(out_file_path, *args, **kwargs)
        except Exception as e:
            logging.info(e)
            raise

    def export(self, out_data: pd.DataFrame, export_to: str = 'auto', out_file_path: str = '',
                server: str = '', db: str = '', out_table: str = '',
                schema: str = 'auto',
                mode: str='write', *args, **kwargs) -> None:
        """
        method for exporting data to csv, excel or SQL server database
        
        Parameters
        out_data: pd.DataFrame
            data to be exported presented as pd.DataFrame
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
        schema: str
            Schema of output table.
            Default is `auto` which means that schema will be infered automatically from `out_data` dataframe
        mode: str
            Specify type of export. Can be one of `append` or `write`.
            `append` will append data to existing table
            `write` will first drop existing table and then create new `out_table`

            Default is `write`

        for additional parameters please refer to:
        https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_excel.html ... for excel
        https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html ... for csv

        """

        if not isinstance(out_data, pd.DataFrame):
            raise Exception('`out_data` must of type `pd.DataFrame`')
        
        assert mode in ['append', 'write'], f'Mode must be one of `append`, `write`'

        assert export_to in ['csv', 'excel', 'db', 'auto'], f'Argument `export_to` must be one of ' \
                                                                   '`csv`, `excel`, `db`, `auto`'

        export_to = self._infer_to(export_to, out_file_path, server, db, out_table)

        mapping = {'excel': 'xlsx', 'csv': 'csv', 'json': 'json'}

        if export_to != 'db':
            if not os.path.splitext(out_file_path)[-1]:
                out_file_path += f'.{mapping[export_to]}'

        if export_to == 'csv':
            self._export_to_csv(out_data, out_file_path, mode, *args, **kwargs)
        elif export_to == 'json':
            self._export_to_json(out_data, out_file_path, mode, *args, **kwargs)
        elif export_to == 'excel':
            self._export_to_excel(out_data, out_file_path, mode, *args, **kwargs)
        elif export_to == 'db':
            try:
                self._export_to_db(out_data, server, db, out_table, schema, mode, *args, **kwargs)
            except Exception as e:
                logging.info(e)
                logging.info('Some SQL error occured. Most probably there is no proper SQL server driver installed. \n '
                             f'Exporting to {os.path.join(os.getcwd(), out_table+".xlsx")} instead.')
                self._export_to_excel(out_data, out_table+'.xlsx', mode, *args, **kwargs)
        else:
            raise Exception(f'Non-implemented for `{export_to}`')
        
    def _create_table(self, schema: str, in_data: pd.DataFrame, out_table: str, cursor: pyodbc.Cursor)-> None:
        """auxiliary method to create new `out_table`, drop old one if already exists
            in_data: pd.DataFrame
                input data which will be used to infer schema (if used `schema=auto`)
            schema: str
                Schema of output table.
            out_table: str
                name of output sql server table
            cursor: pyodbc.Cursor
                pyodbc.Cursor object
        """

        if schema == 'auto':

            dtypes = [str(i).replace('64', '').replace('32', '').replace('16', '').replace('object', 'varchar(max)').replace('str', 'varchar(max)') for i in in_data.dtypes.to_list()]
            cols = list(in_data.columns)
            schema = [f'{col_name} {dtype}' for col_name, dtype in zip(cols, dtypes)]
            schema = ', '.join(schema)
        
        cursor.execute(f"IF OBJECT_ID('{out_table}') IS NOT NULL DROP TABLE {out_table};")
        cursor.execute(f'CREATE TABLE {out_table} ( ' + schema + ' );')
        logging.info(f'Table {out_table} create successfully')

if __name__ == '__main__':

    df = pd.DataFrame({'a': [1, 2, 4, 5], 'b': ['a', 'b', 'c', 'e']})

    csv = r''
    c = Connector() 

    c.load(load_from='auto', in_file_path=csv, encoding='latin-1')
    c.export(df, export_to='csv', out_file_path=r'')