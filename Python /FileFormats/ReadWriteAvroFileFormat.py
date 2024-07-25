import copy
import json
import pandas as pd
import pandavro as pdx
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader

# Data to be saved
users = [
    {'name': 'gowds nasd sk', 'age': 43},
    {'name': 'Jfgdf e sa', 'age': 66}
]

# Convert the list of dictionaries to a pandas DataFrame
users_df = pd.DataFrame.from_records(users)
print(users_df)

# Save the DataFrame to an Avro file using pandavro
pdx.to_avro('../../..Data/users_test.avro', users_df)

# Read the data back from the Avro file into a DataFrame
users_df_redux = pdx.from_avro('../../..Data/users_test.avro')
print(type(users_df_redux))
# <class 'pandas.core.frame.DataFrame'>

# Check the schema for "users.avro"
with open('users.avro', 'rb') as f:
    reader = DataFileReader(f, DatumReader())
    metadata = copy.deepcopy(reader.meta)
    schema_from_file = json.loads(metadata['avro.schema'])
    reader.close()

# Print the schema extracted from the Avro file
print(schema_from_file)
