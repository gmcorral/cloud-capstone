from pyspark import SparkContext
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
            ssc.stop(False, True)
            time.sleep(5)
        except Exception as ignore:
            pass

def signal_handler(signal, frame):
    close()
    sys.exit(0)

######
###### Partial results printer #######
######

def print_rdd(rdd):
    print('=============================')
    airports = rdd.takeOrdered(10, key = lambda x: -x[1])
    for airport in airports:
        print(airport)
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

sc = SparkContext("local[*]", "Test")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("/tmp/checkpoint")

lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

filtered = lines.map(lambda line: line.split(","))\
                .map(lambda fields: Flight(fields))\
                .flatMap(lambda fl: [(fl.Origin, 1), (fl.Dest, 1)])\
                .updateStateByKey(updateFunction)

filtered.foreachRDD(lambda rdd: print_rdd(rdd))

# start streming process
ssc.start()

try:
    ssc.awaitTermination()
except Exception as ex:
    print("ERROR: " + str(ex))
finally:
    print('Shutting down..')
    ssc.stop(False, True)
    time.sleep(5)
