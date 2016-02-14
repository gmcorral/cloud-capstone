from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext

import sys
import signal
import time

from flight import Flight

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

def print_rdd(rdd):
    print('=============================')
    daysOfWeek = rdd.takeOrdered(10, key = lambda x: -x[1])
    for day in daysOfWeek:
        print(day)
    print('=============================')

######
###### Checkpoint status updater #######
######

def updateFunction(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)

######
###### Main script #######
######

signal.signal(signal.SIGINT, signal_handler)

config = SparkConf()
config.set('spark.streaming.stopGracefullyOnShutdown', True)

sc = SparkContext(appName='g1ex3', conf=config, pyFiles=['flight.py'])
ssc = StreamingContext(sc, 1)
ssc.checkpoint('/tmp/g1ex3')

lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

filtered = lines.map(lambda line: line.split(","))\
                .map(lambda fields: Flight(fields))\
                .map(lambda fl: (fl.DayOfWeek, 1))\
                .updateStateByKey(updateFunction)

filtered.foreachRDD(lambda rdd: print_rdd(rdd))

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