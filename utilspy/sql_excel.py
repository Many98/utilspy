from typing import Dict, List
from time import sleep
from tqdm import tqdm

import logging

import pyodbc

import xlsxwriter
#import pandas as pd
import os
#from openpyxl import load_workbook

logging.getLogger().setLevel(logging.INFO)

def sql_create_table(cursor: pyodbc.Cursor, table_name: str , **kwargs) -> None:
    '''Creates new table, drop old one if already exists
        Parameters

        cursor: pyodbc.Cursor
            pyodbc.Cursor object
        table_name: str
            Name of table to be created
        **kwargs:
            Keyword arguments defining schema of created table
    '''
    cursor.execute(f"IF OBJECT_ID('{table_name}') IS NOT NULL DROP TABLE {table_name};")

    sql_string = [f'{key} {value}, ' for key, value in kwargs.items()]
    cursor.execute(f'CREATE TABLE {table_name} ( ' + ''.join(sql_string) + ';')
        #'name varchar(255), '
        #'address varchar(255);')

def sql_insert_data(cursor: pyodbc.Cursor, data: dict, table: str) -> None:
    '''Inserts member information into table
        Parameters
        cursor: pyodbc.Cursor
            pyodbc.Cursor object
        table: str
            Name of table where to insert data
        data: dict
            Dictionary with data to be inserted. It should have structure
            based on schema of table
    '''
    _ = ['?' for i in range(len(data.items))] 
    cursor.execute(f'INSERT INTO {table} VALUES ({", ".join(_)})', *data.values())

def excel_write(name: str, data: List[Dict]) -> None:
    """
    Function to write data to excel.
    Excel will be exported to current working directory.

    Parameters
    name: str
        File name of excel file
    data: list[dict]
        List of dicts where each element (dict) of list represents one row
        and dict encodes structure of one row (values contain data, keys should be column names)
    """
    # Workbook() takes one, non-optional, argument
    # which is the filename that we want to create.
    workbook = xlsxwriter.Workbook(f'{name}.xlsx')
    
    # The workbook object is then used to add new
    # worksheet via the add_worksheet() method.
    worksheet = workbook.add_worksheet()
    
    # Use the worksheet object to write
    # data via the write() method.
    for i, item in enumerate(data):
        for j, (key, value) in enumerate(item.items()):
            if i == 0:
                worksheet.write(i, j, key)
            worksheet.write(i+1, j, value)
    
    # Finally, close the Excel file
    # via the close() method.
    workbook.close()
    logging.info('All saved succesfully !')

#excel_write('test', [{'a': 10, 'b': 'adam', 'c': 45}, {'a': 60, 'b': 'ffam', 'c':65}, {'a': 80, 'b': 'am', 'c':85}])

