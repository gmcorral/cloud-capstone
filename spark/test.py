from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import csv

from StringIO import StringIO

import sys

from flight import Flight

STREAMER_IP = '52.23.49.112'
STREAMER_PORT = 9999

YEAR = 0
MONTH = 1
DAY_OF_MONTH = 2
DAY_OF_WEEK = 3
UNIQUE_CARRIER = 4
FLIGHT_NUM = 5
ORIGIN = 6
DEST = 7
DEP_TIME = 8
DEP_DELAY = 9
ARR_TIME = 10
ARR_DELAY = 11
CANCELLED = 12
        
def load_csv(content):
    return csv.reader(StringIO(contents[1]))

def print_rdd(iter):
    for flight in iter:
        print(flight)
    
# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[*]", "Test")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
stopper = lines.filter(lambda line: line == 'END')\
               .map(lambda end: end)\
               .reduce(lambda end: scc.stop(stopGraceFully=True))
filtered = lines.filter(lambda line: line != 'END')\
                  .map(lambda line: line.split(","))\
                  .map(lambda fields: Flight(fields))\
                  .filter(lambda fl: fl.DayOfWeek == 1)\
                  .saveAsTextFiles('file:///tmp/out', 'txt')
#filtered.foreachRDD(lambda rdd: rdd.foreachPartition(print_rdd))

#counts.pprint()

ssc.start()
ssc.awaitTermination()
#ssc.stop(stopGraceFully=True)
