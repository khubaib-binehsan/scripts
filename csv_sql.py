"Provides functionality to convert CSV file into *.sql PostgreSQL schema file"

from os import path as __path
from re import sub as __sub
import csv


def filename(file_path: str):
    '''
    Returns the name of the file whose path has been passed
    '''

    # uses the os.path function to extract the name of the file
    name = __path.basename(file_path).split('.')[0].lower()

    # returns the name of the file with any whitespace/hypen converted to an underscore
    return __sub(r'[\s-]', '_', name)


def _get_header(
        file_path: str,
        __encoding: str = "utf-8") -> list:
    '''
    Returns the header row of the csv file as a list
    '''

    # opens the file and then loads the file using csv.reader so that it can iterated over
    with open(file_path, newline='', encoding=__encoding) as csvfile:
        reader = csv.reader(csvfile)

        # next() reads the next row, in this case the header row
        # header is a list storing names of all the columns which the data contains
        header = next(reader)

    return header


def _column_def(header: list) -> str:
    '''
    Returns the column definition part of Table creation by returning the columns
    along with SQL's TEXT data type in an easy-to-read, well-formatted string, which
    will be joined with the rest of the parts to create the whole schema and
    data loading.

    . . .

        "col_name" TEXT,

        "col_name" TEXT

    . . .
    '''

    # the items will be first formatted as per the pattern and then will be stored in
    # this temporary list.
    # the list of items will then be joined in such a way that
    # the return value will provide us with the desired formatted string
    temp = []

    for item in header:
        temp.append(f'"{item}" TEXT')

    # \t creates an initial indentation ', \n\t' acts as a delimiter between the items
    # so that each item is placed on the next line with indentation
    return '\t' + ', \n\t'.join(temp)


def _values_str(
        file_path: str,
        __encoding: str = 'utf-8') -> str:
    '''
    Converts all the raw data into a easy-to-read, well-fromatted,
    postgreSQL syntax for inserting data.

    . . . 

        (...'foo', 'bar', 'baz',...),

        (...'foo', 'bar', 'baz',...),

        (...'foo', 'bar', 'baz',...)


    . . .
    '''

    with open(file_path, newline='', encoding=__encoding) as csvfile:
        reader = csv.reader(csvfile)

        # moves the row with one increment so that in the next for loop
        # data starts after the header row
        next(reader)

        # after the necessary formatting of each element, they will be stored in this temporary
        # list which will then be joined together in order to get the desired formatted result
        format_string = []

        # goes through all the rows of csv file and formats the value in each row
        for row in reader:

            # the output will be (item1, item2, item3,...) as a string
            # such that each transformation will be like (item -> 'item')
            # and if there is any ' character it will be replaced with '' since that is how
            # postgreSQL works like ('foo's bar' -> 'foo''s bar')
            tmp = ", ".join(
                [f"""'{str(item).replace("'", "''")}'""" for item in row])

            # lastly the values inside the tmp string are concatenated for the required SQL's
            # syntax additionals and appended to the format_string list
            format_string.append("\n\t(" + tmp + ")")

    # joins all the items inside the format_string list, using a ', ' delimiter
    return ", ".join(format_string)


def csv_postgresql(
        file,
        schema_name: str = "",
        table_name: str = "",
        __encoding: str = 'utf-8') -> None:
    '''
    Converts the csv file into a PostgreSQL *.sql file such that the file contains
    schema creation, table creation, and data loading procedure.

    ________________________________________________________

    CREATE SCHEMA foo

    DROP TABLE IF EXISTS foo.bar;

    CREATE TABLE foo.bar (

        "column" TEXT,

        "column" TEXT
    );

    INSERT INTO foo.bar

        (...,"column","column",...)

    VALUES

        (..'foo','bar',...),

        (..'foo','bar',...),

        (..'foo','bar',...);
    '''

    # if the schema/table name is not given then sets the name to file name
    # if given, then as a fail safe measure, removes whitespace and hypen characters from the names

    # for schema_name:
    if len(schema_name) == 0:
        schema_name = filename(file)
    else:

        # replaces any whitespace/hyphen character with underscore
        schema_name = __sub(r'[\s-]', '_', schema_name).lower()

    # for table_name:
    if len(table_name) == 0:
        table_name = filename(file)
    else:

        # replaces any whitespace/hyphen character with underscore
        table_name = __sub(r'[\s-]', '_', table_name).lower()

    # fetches the header row of the csv
    header = _get_header(file)

    # SCHEMA - TABLE - INSERT
    # schema:
    _schema = f'''CREATE SCHEMA {schema_name}'''

    # table:
    drop_string = f'''DROP TABLE IF EXISTS {schema_name}.{table_name}'''
    create_string = f'''CREATE TABLE {schema_name}.{table_name}'''
    definition_string = _column_def(header)

    _table = f'''{drop_string};\n{create_string} (\n{definition_string}\n)'''

    # insert:
    imain_string = f'''INSERT INTO {schema_name}.{table_name}'''

    icolumns_string = ', '.join(
        [f'"{item}"' for item in header])

    ivalues_string = _values_str(file)

    _insert = f'''{imain_string}\n\t({icolumns_string})\nVALUES{ivalues_string}'''

    # OUTPUT STRING
    ouput_string = f'''{_schema};\n\n{_table};\n\n{_insert};'''

    # SQL FILE WRITING

    output_name = filename(file) + '.sql'

    with open(output_name, 'w', encoding=__encoding) as output:
        output.write(ouput_string)
