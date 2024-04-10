
import pandas as pd
from pandas import json_normalize
import json

df = pd.read_csv('proj1_ex01.csv')

columns_info = []

total_rows = len(df)
for column in df.columns:
    if 'int' in str(df[column].dtype):  
        type = "int"
    elif 'float' in str(df[column].dtype):  # Check if the string contains 'float'    
        type = "float"
    else:
        type = "other"
    column_info = {
        "name": column,
        "missing": df[column].isnull().mean(),
        "type": type
    }
    columns_info.append(column_info)

with open('proj1_ex01_fields.json', 'w') as output_file:
    json.dump(columns_info, output_file, indent=4)

import numpy as np

class NpEncoder(json.JSONEncoder): 
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):

            return int(obj)

        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)

        elif isinstance(obj, (np.complex_, np.complex64, np.complex128)):
            return {'real': obj.real, 'imag': obj.imag}

        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()

        elif isinstance(obj, (np.bool_)):
            return bool(obj)

        elif isinstance(obj, (np.void)):
            return None

        return json.JSONEncoder.default(self, obj)

statistics = {}

for column in df.columns:
    if df[column].dtype == 'object':
        stats = {
            "count": df[column].count(),
            "unique": df[column].nunique(),
            "top": df[column][0],
            "freq": df[column].value_counts().max()
        }        
    else:
        stats = {
            "count": df[column].count(),
            "mean": df[column].mean(),
            "std": df[column].std(),
            "min": df[column].min(),
            "25%": df[column].quantile(0.25),
            "50%": df[column].quantile(0.50),
            "75%": df[column].quantile(0.75),
            "max": df[column].max()
        }
    statistics[column] = stats

with open('proj1_ex02_stats.json', 'w') as output_file:
    json.dump(statistics, output_file, indent=4, cls=NpEncoder)

new_columns = {}
for column in df.columns:
    new_name = ''.join(c.lower() if c.isalnum() or c == ' ' else ''
                       for c in column)
    new_name = new_name.replace(' ', '_')
    new_columns[column] = new_name

df.rename(columns=new_columns, inplace=True)

df.to_csv('proj1_ex03_columns.csv', index=False)

df.head()

df.to_excel('proj1_ex04_excel.xlsx', index=False)

df.to_json('proj1_ex04_json.json', orient='records', indent=4)

df.to_pickle('proj1_ex04_pickle.pkl')

df = pd.read_pickle('proj1_ex05.pkl')


selected_columns = df.iloc[:, 1:3]

selected_rows = selected_columns[df.index.str.startswith('v')]

selected_rows = selected_rows.fillna(' ')

markdown_table = selected_rows.to_markdown(index=True)

with open('proj1_ex05_table.md', 'w') as output_file:
    output_file.write(markdown_table)



with open('proj1_ex06.json', 'r') as input_file:
    data = json.load(input_file)

df = json_normalize(data, sep='.')

df.to_pickle('proj1_ex06_pickle.pkl')