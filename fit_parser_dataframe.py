
# Integration with pandas

import pandas as pd
from fit_parser import GlobalMessageDecl, RecordDecl, parse_fit_file

# TODO: handle missing columns by raising the appropriate Error object
# TODO: handle wildcarding all available columns

def parse_fit_as_dataframe(path, columns):
    """Parse a .fit file and return a Pandas DataFrame object with the specified columns.

    Parameters
    ----------
    path: str
        Path to the .fit file
    columns: list[int]
        A list of field ids to include in the data frame. The field ids are typically in the form of enums for
        the record type. For example, if you want to read the heart rate from a Record message, you will
        typically use RecordDecl.heart_rate. In the end, it is just an integer that is used.

    Returns
    -------
    pandas.DataFrame
        A Pandas dataframe object that contains the requested columns from the .fit file.

    Examples
    --------
    >>> fit_file = parse_fit_as_dataframe('fit_file.fit', [RecordDecl.heart_rate, RecordDecl.power])
    """

    # Initialize a dictionary containing names of columns names and empty lists
    data = {}
    for column in columns:
        data[column.name] = []

    for message in parse_fit_file(path, validate_crc = False):
        if message.message_definition.global_message_number == GlobalMessageDecl.record:
            for column in columns:
                # Special case time stamp
                if column is RecordDecl.time_stamp:
                    data[column.name].append(message.get_as_datetime(column))
                else:
                    data[column.name].append(message.get(column))

    return pd.DataFrame(data)