import pandas as pd
import json

data1 = pd.read_json('proj3_data1.json')
data2 = pd.read_json('proj3_data2.json')
data3 = pd.read_json('proj3_data3.json')

all_data = pd.concat([data1, data2, data3], ignore_index=True)

all_data.to_json('proj3_ex01_all_data.json', indent=4)

all_data = pd.read_json('proj3_ex01_all_data.json')

missing_values = all_data.isnull().sum()

missing_values = missing_values[missing_values > 0]

missing_df = pd.DataFrame({
    'column_name': missing_values.index,
    'missing_count': missing_values.values
})

missing_df.to_csv('proj3_ex02_no_nulls.csv', index=False, header=False)

all_data = pd.read_json('proj3_ex01_all_data.json')

with open('proj3_params.json', 'r') as file:
    params = json.load(file)

concat_columns = params['concat_columns']

all_data['description'] = all_data[concat_columns].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)

all_data.to_json('proj3_ex03_descriptions.json', indent=4)

all_data = pd.read_json('proj3_ex03_descriptions.json')

more_data = pd.read_json('proj3_more_data.json')

with open('proj3_params.json', 'r') as file:
    params = json.load(file)

join_column = params['join_column']

joined_data = all_data.merge(more_data, on=join_column, how='left')

joined_data.to_json('proj3_ex04_joined.json', indent=4)

joined_data = pd.read_json('proj3_ex04_joined.json')

with open('proj3_params.json', 'r') as file:
    params = json.load(file)

int_columns = params['int_columns']

for col in int_columns:
    joined_data[col] = joined_data[col].fillna(pd.NA).astype(float).astype('Int64')

for index, row in joined_data.iterrows():
    description = row['description'].lower().replace(' ', '_')

    filename = f"proj3_ex05_{description}.json"

    row_data = row.drop('description').to_dict()

    with open(filename, 'w') as file:
        row_data = {k: v if pd.notna(v) else None for k, v in row_data.items()}
        json.dump(row_data, file, indent=4)

    for col in int_columns:
        if col in row_data:
            if pd.isna(row_data[col]):
                row_data[col] = None
            else:
                row_data[col] = int(row_data[col])

    filename = f"proj3_ex05_int_{description}.json"

    with open(filename, 'w') as file:
        json.dump(row_data, file, indent=4)

joined_data = pd.read_json('proj3_ex04_joined.json')

with open('proj3_params.json', 'r') as file:
    params = json.load(file)

aggregations = params['aggregations']

agg_results = {}

for col, func in aggregations:
    if func == 'min':
        result = joined_data[col].min()
        key = f"min_{col}"
    elif func == 'max':
        result = joined_data[col].max()
        key = f"max_{col}"
    elif func == 'mean':
        result = joined_data[col].mean()
        key = f"mean_{col}"
    else:
        continue

    agg_results[key] = result

with open('proj3_ex06_aggregations.json', 'w') as file:
    json.dump(agg_results, file, indent=4)

joined_data = pd.read_json('proj3_ex04_joined.json')

with open('proj3_params.json', 'r') as file:
    params = json.load(file)

grouping_column = params['grouping_column']

grouped_data = joined_data.groupby(grouping_column)

filtered_data = grouped_data.filter(lambda x: len(x) > 1)

numeric_columns = filtered_data.select_dtypes(include=['float64', 'int64']).columns
mean_values = filtered_data.groupby(grouping_column)[numeric_columns].mean()

mean_values.to_csv('proj3_ex07_groups.csv', index=True)

joined_data = pd.read_json('proj3_ex04_joined.json')

with open('proj3_params.json', 'r') as file:
    params = json.load(file)

pivot_index = params['pivot_index']
pivot_columns = params['pivot_columns']
pivot_values = params['pivot_values']

pivot_df = joined_data.pivot_table(index=pivot_index, columns=pivot_columns, values=pivot_values, aggfunc='max')

pivot_df.to_pickle('proj3_ex08_pivot.pkl')

id_vars = params['id_vars']

melted_df = pd.melt(joined_data, id_vars=id_vars, var_name='variable', value_name='value')

melted_df.to_csv('proj3_ex08_melt.csv', index=False)

statistics_df = pd.read_csv('proj3_statistics.csv')

df = pd.DataFrame(statistics_df)

df_long = pd.wide_to_long(df,
                          stubnames=["Audi", "BMW", "Volkswagen", "Renault"],
                          i=["Country"],
                          j="Year",
                          sep="_",
                          suffix='\d+')

df_long.to_pickle('proj3_ex08_stats.pkl')
