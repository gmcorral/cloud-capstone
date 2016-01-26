import csv
import os
import zipfile
import boto3
from datetime import datetime, timedelta

# S3 region and bucket name
AWS_REGION = 'us-east-1'
S3_BUCKET = 'airline-ontime'

# range of years to parse
YEAR_RANGE = range(1988, 2009)

# 0-based index of date field
DATE_INDEX = 5

# list of 0-based indexes to import from csv
# 0: "Year","Quarter","Month","DayofMonth","DayOfWeek","FlightDate","UniqueCarrier","AirlineID","Carrier","TailNum",
# 10: "FlightNum","Origin","OriginCityName","OriginState","OriginStateFips","OriginStateName","OriginWac","Dest","DestCityName","DestState",
# 20: "DestStateFips","DestStateName","DestWac","CRSDepTime","DepTime","DepDelay","DepDelayMinutes","DepDel15","DepartureDelayGroups","DepTimeBlk",
# 30: "TaxiOut","WheelsOff","WheelsOn","TaxiIn","CRSArrTime","ArrTime","ArrDelay","ArrDelayMinutes","ArrDel15","ArrivalDelayGroups",
# 40: "ArrTimeBlk","Cancelled","CancellationCode","Diverted","CRSElapsedTime","ActualElapsedTime","AirTime","Flights","Distance","DistanceGroup",
# 50: "CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay","FirstDepTime","TotalAddGTime","LongestAddGTime","DivAirportLandings","DivReachedDest",
# 60: "DivActualElapsedTime","DivArrDelay","DivDistance","Div1Airport","Div1WheelsOn","Div1TotalGTime","Div1LongestGTime","Div1WheelsOff","Div1TailNum","Div2Airport",
# 70: "Div2WheelsOn","Div2TotalGTime","Div2LongestGTime","Div2WheelsOff","Div2TailNum",

# Year - Month - DayofMonth - DayOfWeek - UniqueCarrier - Origin - Dest - DepDelay - ArrDelay - Cancelled
IMPORT_FIELDS = [0, 2, 3, 4, 6, 11, 17, 25, 36, 41]

# CSV separator and string delimiter
INPUT_SEP = ','
STR_DELIM = '"'

# file name and extension
FILE_NAME = 'On_Time_On_Time_Performance_'
FILE_ZIP_EXT = '.zip'
FILE_EXT = '.csv'

# base input path
BASE_PATH = '/mnt/airdb/aviation/airline_ontime'

# temp path to extract files
TMP_DIR = '/tmp'


def read_month_csv(path):

    month_dict = dict()
    
    try:
        with open(path, 'rb') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(csv_reader, None)
            for row in csv_reader:
                row_values = []
                for index in IMPORT_FIELDS:    
                    if index >= 0 and index < len(row):
                        value = row[index]
                        if value is not None:
                            row_values.append(value)
                        else:
                            row_values.append(u'\\N')
                date = row[DATE_INDEX]
                if date in month_dict:
                    month_dict[date].append(row_values)
                else:
                    month_dict[date] = [row_values]
    except Exception as ex:
        print 'Error reading month: ' + str(ex)
    
    return month_dict
    
    
def dump_month(month):

    for day in month.keys():
    
        s3_key = 'date=' + day + '/001.csv'
        
        print 'Dumping ' + s3_key
        
        try:
            values = month[day]
            s3_content = u'\n'.join([u','.join([value for value in line]) for line in values])
            s3_object = s3.Object(S3_BUCKET, s3_key).put(Body=s3_content)
        except Exception as ex:
            print 'Error dumping values to S3' + str(ex)

def extract_file(path):

    print 'Extracting ' + path
    
    try:
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(TMP_DIR)
            return True
    except Exception as ex:
        print 'Error extracting file' + str(ex)
        return False


def read_month(year, month):
    
    print 'Reading ' + year + '-' + month
    
    input_path = os.path.join(BASE_PATH, year, FILE_NAME + year + '_' + month + FILE_ZIP_EXT)
    extracted_path = os.path.join(TMP_DIR, FILE_NAME + year + '_' + month + FILE_EXT)
    
    if extract_file(input_path):
        month_dict = read_month_csv(extracted_path)
        dump_month(month_dict)
        os.remove(extracted_path)


def read_year(year):
    for month in range(1, 13):
        read_month(year, str(month))
    
###################
# Script starts here
###################

s3 = boto3.resource('s3', region_name=AWS_REGION)

for year in YEAR_RANGE:
    read_year(str(year))
