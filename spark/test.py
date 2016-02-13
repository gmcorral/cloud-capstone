from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import sys

from flight import Flight

def print_rdd(iter):
    for flight in iter:
        print(flight)

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[*]", "Test")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

# stop when END is received?
#stopper = lines.filter(lambda line: line == 'END')\
#               .map(lambda end: end)\
#               .reduce(lambda end: scc.stop(stopGraceFully=True))

filtered = lines.filter(lambda line: line != 'END')\
                  .map(lambda line: line.split(","))\
                  .map(lambda fields: Flight(fields))\
                  .filter(lambda fl: fl.DayOfWeek == 1)\
                  .saveAsTextFiles('file:///tmp/out', 'txt')

# print instead of dump
#filtered.foreachRDD(lambda rdd: rdd.foreachPartition(print_rdd))

ssc.start()
ssc.awaitTermination()
#ssc.stop(stopGraceFully=True)
