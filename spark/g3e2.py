from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext

import sys
import signal
import time

from flight import Flight

from boto import dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item

import datetime

# DynamoDB region and bucket name
AWS_REGION = 'us-east-1'
DB_TABLE = 't2g3ex2'

scc = None

######
###### Closing handler #######
######

def close():
    print('Shutting down..')
    if (scc is not None):
        try:
            ssc.stop(True, True)
        except:
            pass

def signal_handler(signal, frame):
    close()

######
###### Partial results printer #######
######

def save_partition(part):

    for record in part:
    
        fl_xy = record[1][0]
        fl_yz = record[1][1]
        route = fl_xy.Origin + '-' + fl_xy.Dest + '-' + fl_yz.Dest
        depdate = record[0][0]
        
        item_new = Item(out_table, data={
            "route": route,
            "depdate": depdate,
            "flight_xy": fl_xy.UniqueCarrier + str(fl_xy.FlightNum),
            "flight_yz": fl_yz.UniqueCarrier + str(fl_yz.FlightNum),
            "total_delay": int(fl_xy.DepDelay + fl_xy.ArrDelay + fl_yz.DepDelay + fl_yz.ArrDelay)
        })
        
        # check old item delay
        try:
            item_old = out_table.get_item(route=route, depdate = depdate)
            if (item_old['total_delay'] > item_new['total_delay']):
                item_new.save(overwrite=True)
        except:
            item_new.save(overwrite=True)

######
###### Mapper and reducer functions for XY flights #######
######

def map_flight(flight, is_yz=False):
    flight_date = datetime.date(flight.Year, flight.Month, flight.DayofMonth)
    y_flight = flight.Dest
    if (is_yz):
        flight_date -= datetime.timedelta(days=2)
        y_flight = flight.Origin
    return ((str(flight_date), y_flight), flight)


def reduce_flight(fl1, fl2):
    fl1_delay = fl1.DepDelay + fl1.ArrDelay
    fl2_delay = fl2.DepDelay + fl2.ArrDelay
    return fl1 if fl1_delay <= fl2_delay else fl2
    
    
######
###### Main script #######
######

signal.signal(signal.SIGINT, signal_handler)

dynamo = dynamodb2.connect_to_region(AWS_REGION)
out_table = Table(DB_TABLE, connection = dynamo)

config = SparkConf()
config.set('spark.streaming.stopGracefullyOnShutdown', True)
#config.set('spark.yarn.executor.memoryOverhead', '2g')

sc = SparkContext(appName='g3ex2', conf=config, pyFiles=['flight.py'])
ssc = StreamingContext(sc, 1)
ssc.checkpoint('/tmp/g3ex2')

lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

#initial filter
filtered = lines.map(lambda line: line.split(","))\
                .map(lambda fields: Flight(fields))\
                .filter(lambda fl: fl.Cancelled == 0)
               
# filter XY candidates 
flights_xy = filtered.filter(lambda fl: fl.CRSDepTime < "1200")\
                .map(map_flight)
                #.reduceByKey(reduce_flight)

# filter YZ candidates
flights_yz = filtered.filter(lambda fl: fl.CRSDepTime > "1200")\
                .map(lambda fl: map_flight(fl, True))
                #.reduceByKey(reduce_flight)

# join both legs
flights_xyz = flights_xy.join(flights_yz)

# save to DB
flights_xyz.foreachRDD(lambda rdd: rdd.foreachPartition(save_partition))

# start streaming process
ssc.start()

try:
    ssc.awaitTermination()
except:
    pass

try:
    time.sleep(10)
except:
    pass