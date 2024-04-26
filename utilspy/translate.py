from tqdm import tqdm
import pandas as pd
import pyodbc
import sqlalchemy as sql
from typing import Dict, List, Any

import logging

# -------------------------------------------------------------------
# small boiler plate for importing from modules higher in hierarchy
# + use absolute imports
import sys
from pathlib import Path
file = Path(__file__).resolve()
root = str(file).split('utilspy')[0] + 'utilspy'
sys.path.append(root)
# -------------------------------------------------------------------

# from toolspy.utils.data_sample_to_excel import get_data_from_server

from utilspy.Connector import Connector

logging.getLogger().setLevel(logging.INFO)


def translate_data(data: pd.DataFrame, text_column_name: str, server: str, database: str, out_table_name: str,
                    src_lang: str='cs', dest_lang: str='de') -> pd.DataFrame:
    """
    function to translate string at `text_column_name` column in input DataFrame `data` from `src_lang` to `dest_lang`.
    Translated strings are inserted to SQL server table named `out_table_name`.

    Parameters:
    data: pd.DataFrame
        Input data which have column named `text_column_name` and contains strings to be translated
    text_column_name: str
        Name of column where are string to be translated
    server: str
        server name (server in local network)
    database: str
        Name of sql server database
    out_table_name: str
        name of sql server output table (will be droped if already exists)
    src_lang: str
        Specifies source language
    dest_lang: str
        Specifies destination language

    Returns 
        DataFrame with translated data
    """
    engine = sql.create_engine(f'mssql+pyodbc://{server}/{database}?driver=SQL+Server')
    try:
        from googletrans import Translator
    except ImportError as e:
        logging.info(f'`googletrans` is specific package needed to run this script. Please install it prior to using this script.'
                      f' For installation details please refer to https://pypi.org/project/googletrans/ \n'
                      f'{e}')
        raise

    translator = Translator()
    l = []
    for i, row in tqdm(data.iterrows(), desc="translating data...", total=len([i for i in data.iterrows()])):
        try:
            l.append(translator.translate(row[text_column_name], dest=dest_lang, src=src_lang).text)
        except Exception:
            continue
    data.update(pd.DataFrame({f'{text_column_name}_translated_to_{dest_lang}': l}))
    data.to_sql(out_table_name, engine, if_exists='replace')  # this creates new table

    # this needs to be generalized 
    '''
    sql = f"""
            UPDATE {table_name} AS f 
            SET long_txt = a.long_txt
            FROM tmp_table a join {table_name} b on a.code = b.code
           """
    with engine.begin() as conn:     # TRANSACTION
        conn.execute(sql)
    '''

    return data


if __name__ == '__main__':
    # data = get_data_from_server(server='', database='', table_name='')
    c = Connector()
    data = c.load(server='', db='', in_table='')
    d = translate_data(data, text_column_name='long_txt', server='', database='', out_table_name='')
    print(data)