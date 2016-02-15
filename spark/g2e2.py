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

# DynamoDB region and bucket name
AWS_REGION = 'us-east-1'
DB_TABLE = 't2g2ex2'

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
        item = Item(out_table, data={
            "airport": record[0][0],
            "destination": record[0][1],
            "mean_delay": int(record[1][0] / record[1][1])
        })
        item.save(overwrite=True)
    
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

dynamo = dynamodb2.connect_to_region(AWS_REGION)
out_table = Table(DB_TABLE, connection = dynamo)

config = SparkConf()
config.set('spark.streaming.stopGracefullyOnShutdown', True)
#config.set('spark.yarn.executor.memoryOverhead', '2g')

sc = SparkContext(appName='g2ex2', conf=config, pyFiles=['flight.py'])
ssc = StreamingContext(sc, 1)
ssc.checkpoint('/tmp/g2ex2')

lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

filtered = lines.map(lambda line: line.split(","))\
                .map(lambda fields: Flight(fields))\
                .filter(lambda fl: fl.Cancelled == 0)\
                .map(lambda fl: ((fl.Origin, fl.Dest), (fl.DepDelay, 1)))\
                .updateStateByKey(updateFunction)

filtered.foreachRDD(lambda rdd: rdd.foreachPartition(save_partition))

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