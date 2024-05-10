<h1><p align="center">:candy:utilspy:candy:</p></h1>

<p align="center">utilspy is small package with helpful python modules related to my work. contains few utilities mainly for importing/exporting data from/to files like excel, csv or SQL server database.
    Most recent and useful functionality is `Connector` interface which serves as generic interface for importing/exporting data.
    It is recommended to use `Connector` interface for new classes.</p>



# Installation & requirements

## Clone & play
To clone repo and play with it you can use just

-   ```bash
    git clone https://github.com/Many98/utilspy.git 
    ```

    or (on Windows) use

-   [Github Desktop](https://desktop.github.com/)


## Package
To install `utilspy` as package use:

-  ```bash
    pip install git+https://github.com/Many98/utilspy
    ```

    or 

-   ```bash
    git clone https://github.com/Many98/utilspy.git  && cd utilspy && pip install -e .
    ```

If you wish to install without dependencies
use `--no-deps` flag in your `pip install command`
e.g.

```bash
git clone https://github.com/Many98/utilspy.git  && cd utilspy && pip install --no-deps -e .
```



## Examples:

* #### Connector
    Implement custom `MyClass` which inherits all `Connector`'s functionality.
    Then initialize instance `mc` of `MyClass`, load some data from excel file and export
    another data to `out_table`.

    ```python

    import utilspy
    import pandas as pd

    class MyClass(utilspy.Connector):
        def __init__() -> None:
            super().__init__()
            
    mc = MyClass() # have now access to handy load/export methods of `Connector`

    data = mc.load(in_file_path=<some excel file>)  # load from some excel file
    mc.export(out_data=pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}),
            server=<server name in local network>, db=<sql server database>, out_table=<output table name>,
            mode='write',
            schema='auto' 
            )  # export data to sql server database (newly created `out_table`) and infer schema from data

    ```
* #### translate_data
    Load data to be translated from `in_table` using `Connector` and then translate column `text_column_name` from czech language to
    german language. Finally export result to `out_table_name`.

    ```python

    import utilspy
    c = utilspy.Connector()
    data = c.load(server=<server name in local network>, db=<sql server database>, in_table=<input table name>)
    d = utilspy.translate.translate_data(data, text_column_name=<name of column where are stored texts to be translated>, 
                                        server=<server name in local network>,
                                        database=<sql server database>, out_table_name=<output table name>,
                                        src_lang='cs', dest_lang='de')
    print(d)

    ```
