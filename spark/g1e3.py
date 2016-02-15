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
    daysOfWeek = rdd.takeOrdered(10, key = lambda x: x[1][0]/x[1][1])
    for day in daysOfWeek:
        print('(' + str(day[0]) + ', ' + str(day[1][0] / day[1][1]) + ')')
    print('=============================')

######
###### Checkpoint status updater #######
######

def updateFunction(new_values, last_sum):

    new_vals0 = 0.0
    new_vals1 = 0
    
    for val in new_values:
        new_vals0 += val[0]
        new_vals1 += val[1]
    
    last_vals0 = last_sum[0] if last_sum is not None else 0.0
    last_vals1 = last_sum[1] if last_sum is not None else 0
    
    return (new_vals0 + last_vals0,\
            new_vals1 + last_vals1) 

######
###### Main script #######
######

signal.signal(signal.SIGINT, signal_handler)

config = SparkConf()
config.set('spark.streaming.stopGracefullyOnShutdown', True)
#config.set('spark.yarn.executor.memoryOverhead', '2g')

sc = SparkContext(appName='g1ex3', conf=config, pyFiles=['flight.py'])
ssc = StreamingContext(sc, 1)
ssc.checkpoint('/tmp/g1ex3')

lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

filtered = lines.map(lambda line: line.split(","))\
                .map(lambda fields: Flight(fields))\
                .filter(lambda fl: fl.Cancelled == 0)\
                .map(lambda fl: (fl.DayOfWeek, (fl.ArrDelay, 1)))\
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