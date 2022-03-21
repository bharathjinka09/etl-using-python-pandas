import glob 
import pandas as pd 
from datetime import datetime

list_csv = glob.glob("dealership_data/*.csv")

print(list_csv)

list_json = glob.glob("dealership_data/*.json")

print(list_json)


# Logging entries
from datetime import datetime

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open("logfile.txt",'a') as f:
        f.write(timestamp + ',' + message + '\n')



# extract from csv
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

df = extract_from_csv('dealership_data\\used_car_prices2.csv')

print(df)


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe

df = extract_from_csv('dealership_data\\used_car_prices2.json')

print(df)


# Extract data from files
def extract():
    # create an empty dataframe to hold extracted data

    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])

    # process all csv files
    for csvfile in glob.glob("dealership_data/*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile),ignore_index=True)

    # process all csv files
    for jsonfile in glob.glob("dealership_data/*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile),ignore_index=True)

    return extracted_data

log('ETL Job started')
log('Extract phase started')
extracted_data = extract()
log('Extract phase ended')


# Transform Data
def transform(data):
    data['price'] = data['price'].astype(int)

    return data



log('Transform job started')
transformed_data = transform(extracted_data)
print(transformed_data)
log('Transform job ended')


# Load Data
def load(target_file,data_to_load):
    data_to_load.to_csv(target_file)

target_file = 'transformed_data.csv'

log('Load job started')
load(target_file,transformed_data)
log('Load job ended')

log('ETL job ended')







































