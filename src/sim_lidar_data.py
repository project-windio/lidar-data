import zmq
import logging
import sys
import pickle
from time import sleep
from time import time
import calendar
from datetime import datetime
from datetime import timezone
import uptime

try:
    from sim_lidar_data.py import (init, LIDAR_TOPIC)
except ImportError:
    raise Exception ("failed to import init method or topic")
    sys.exit(-1)

def gen_lidar_message():
    data =  [str(calendar.timegm(datetime.fromtimestamp(timestamp=time(), tz=timezone.utc).utctimetuple())), uptime.uptime(), 2027075.0, '2022-06-15 08:49:22+00:00',
             200.0, 2027075.0, 1655282962, 3.4464, 0.2488, 190.9362, 180.0, 2027076.0,
             1655282963, 4.0556, -0.0248, 183.9466, 160.0, 2027077.0, 1655282964, 3.9232, -0.0083, 185.4916, 140.0, 2027078.0, 1655282965, 3.8143, 0.038,
             183.4078, 120.0, 2027079.0, 1655282966, 3.4729, 0.0513, 184.1194, 100.0, 2027080.0, 1655282967, 2.8372, -0.0333, 180.4605, 80.0, 2027081.0,
             1655282968, 2.6685, -0.1191, 177.5728, 60.0, 2027082.0, 1655282969, 2.7555, -0.2547, 187.6137, 40.0, 2027083.0, 1655282970, 9999.0, 9999.0,
             9999.0, 20.0, 2027084.0, 1655282971, 3.3366, -0.1468, 160.2561, 0.0, 2027085.0, 1655282972, 9999.0, 9999.0, 9999.0, 22.6, 12.28, 1021.6, 0.31,
             0.0, 41.9, 0.0, 213.2, 38.0, 38.0, 23.0, 53.11, 8.86, 1.0]
    return data


def initiate_zmq_connection():
    config = init()
    zmq_connection = f'{config["ipc_protocol"]}:{config["ipc_port"]}'
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    logging.debug(f'binding to {zmq_connection} for zeroMQ IPC')
    try:
        socket.bind(zmq_connection)
        print(f"successfully established zmq connection")
    except Exception as e:
        logging.fatal('failed to connect to zeroMQ socket for IPC')
        sys.exit(-1)
    logging.debug(f'connected to zeroMQ IPC socket')
    logging.debug(f'entering endless loop')
    LIDAR_TOPIC = "ldr".encode('utf-8')  # setting the Topic for ZMQ
    while True:
        try:
            lidar_data = socket.send_multipart([LIDAR_TOPIC, pickle.dumps(gen_lidar_message())])
        except Exception as e:
            print(f"unable to send data with zmq: {e}")
            sys.exit(-1)
        sleep(1)