
# Integration with pandas

import pandas as pd
from fit_parser import GlobalMessageDecl, RecordDecl, parse_fit_file

def parse_fit_as_dataframe(path, columns):

    # Initialize a dictionary containing names of columns names and empty lists
    data = {}
    for column in columns:
        data[column.name] = []

    for message in parse_fit_file(path):
        if message.message_definition.global_message_number == GlobalMessageDecl.record:
            for column in columns:
                # Special case time stamp
                if column is RecordDecl.time_stamp:
                    data[column.name].append(message.get_as_datetime(column))
                else:
                    data[column.name].append(message.get(column))

    return pd.DataFrame(data)