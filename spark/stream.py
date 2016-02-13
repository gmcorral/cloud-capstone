import socket
import boto
from boto.s3.key import Key
import signal
import sys

# S3 region and bucket name
AWS_REGION = 'us-east-1'
S3_BUCKET = 'airline-test'

# file name and extension
FILE_NAME = '001.csv'

# range of years to parse
YEAR_RANGE = range(2008, 2009)
MONTH_RANGE = range(1, 2)

# connection to Spark
PORT = 9999

sock = None
conn = None

def signal_handler(signal, frame):
    print('Exiting..')
    disconnect()
    sys.exit(0)

def listen(port):
    sock = socket.socket()
    sock.bind((socket.gethostname(), port))
    sock.listen(5)
    return sock

def disconnect():
    
    if(conn is not None):
        print('Closing connection')
        conn.close()
        
    if(sock is not None):
        print('Closing socket')
        sock.close()

def send_file(conn, content):

    #lines = content.split('\n')
    #for line in lines:
    conn.send(content)
    #    print('Sent ', line)
    print('file sent')
    
    
def send_day(conn, bucket, day_key):
    
    try:
        
        s3_key = Key(bucket)
        s3_key.key = day_key
        if(s3_key.exists()):
            print 'Sending ' + day_key
            send_file(conn, s3_key.get_contents_as_string())
        else:
            print ' - Skipping ' + day_key
       
    except Exception as ex:
        print 'Error dumping values to S3' + str(ex)

def send_month(conn, bucket, year, month):
    
    print ('Reading ' + year + '-' + month)
    
    for day in range(1, 31):
        day_str = "%02d" % (day,)
        send_day(conn, bucket, 'date=' + year + '-' + month + '-' + day_str + '/' + FILE_NAME)

def send_year(conn, bucket, year):
    for month in MONTH_RANGE:
        month_str = "%02d" % (month,)
        send_month(conn, bucket, year, month_str)
    
###################
# Script starts here
###################

signal.signal(signal.SIGINT, signal_handler)

try:
    s3 = boto.s3.connect_to_region(region_name=AWS_REGION)
    bucket = s3.get_bucket(S3_BUCKET)

    sock = listen(PORT)

    print('Waiting for connections..')

    conn, addr = sock.accept()

    print('Connection from ' + str(addr))

    for year in YEAR_RANGE:
        send_year(conn, bucket, str(year))
        
    print('Finished sending files')
    
    conn.send('\nEND\n')
    #disconnect()

except Exception as ex:
    print(ex)
    disconnect()
    