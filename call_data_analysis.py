#!/usr/bin/env python
# coding: utf-8

# # Analysis of call data


import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader
import csv
from flatten_dict import flatten
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


inputFile = 'CT_Sample.avro'
reader = DataFileReader(open(inputFile,"rb"),DatumReader())
schema = reader.meta
input_data_schema = schema['avro.schema'].decode('latin1')
print(input_data_schema)


# ### Flatten the data using '_' separator, compute average and max RxLevel

def underscore_partitioner(key1, key2):
    if key1 is None:
        return key2
    else:
        return key1 + "_" + key2

csv_columns = set()
flattenedData = list()
i = 0
sum = 0
max_RxLevel = None
average_RxLevel = None

for topLevelRecord in reader:
    csv_columns = csv_columns.union(set(flatten(topLevelRecord, reducer=underscore_partitioner).keys()))
    flattenedData.append(flatten(topLevelRecord, reducer=underscore_partitioner))
    if (topLevelRecord['Simoperator'] == '26202'):
        sum = sum + topLevelRecord['RxLevel']
        if (i == 0):
            max_RxLevel = topLevelRecord['RxLevel']
        elif (topLevelRecord['RxLevel'] > max_RxLevel):
            max_RxLevel = topLevelRecord['RxLevel']        
        i+=1
        
if (sum != 0 and i != 0):
    average_RxLevel = sum / i

print('max_RxLevel',max_RxLevel)
print('average_RxLevel',average_RxLevel)

# ### Write the content into a csv file

outputCSVFile = re.sub(r'\..+','',inputFile) + '.csv'
try:
    with open(outputCSVFile, 'w') as file:
        csv_writer = csv.DictWriter(file, fieldnames=list(csv_columns))
        csv_writer.writeheader()
        csv_writer.writerows(flattenedData)
except IOError:
    print("I/O error") 


# ### Write the content into parquet file with partition as first 3 characters of 'Simoperator' field

outputParquetFile = re.sub(r'\..+','',inputFile)# + '.parquet'
df_call_data = pd.read_csv(outputCSVFile)
df_call_data['SimOperator_key'] = df_call_data['Simoperator'].apply(lambda x : str(x)[:3])
#print(df_call_data.head())


df_call_data_table = pa.Table.from_pandas(df_call_data, preserve_index=False)
#pq.write_table(df_call_data_table, outputParquetFile)
pq.write_to_dataset(df_call_data_table, root_path=outputParquetFile, partition_cols=['SimOperator_key'])


# ### Read from the saved parquet file into pandas dataframe

print(pq.read_table(outputParquetFile).to_pandas().drop(columns='SimOperator_key'))


