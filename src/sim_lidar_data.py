import struct
import zmq
from pyModbusTCP.client import ModbusClient
from time import sleep
from datetime import datetime, timezone
from time import time
import logging
import sys
import uptime
import pickle
import calendar
from random import uniform

class Lidar():
    def __init__(self):
        """
        Prepare ZMQ connection and importing necessary elements of lidar_data_config.py
        """
        try:
            from sim_lidar_data_config import (init, LIDAR_TOPIC)
            self.lidar_topic = LIDAR_TOPIC
        except ImportError:
            raise Exception ("failed to import init method or topic")
            sys.exit(-1)
        config = init()
        #self.client = ModbusClient(host=config["modbus_protocol"], port=config["modbus_port"], auto_open=True)
        self.zmq_connection= f'{config["ipc_protocol"]}:{config["ipc_port"]}'
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)

    def bind_zmq_connection(self):
        """
        Bind a zmq connection for intern communication. The data extracted from the lidar
        is published and is received by lidar_mqtt.py.
        """

        logging.debug(f'binding to {self.zmq_connection} for zeroMQ IPC')
        try:
            self.socket.bind(self.zmq_connection)
            print(f"successfully established zmq connection")
        except Exception as e:
            logging.fatal('failed to connect to zeroMQ socket for IPC')
            sys.exit(-1)
        logging.debug(f'connected to zeroMQ IPC socket')

    def publish_data(self):
        """
        Publish all data using ZeroMQ.
        """
        try:
            lidar_data = self.socket.send_multipart([self.lidar_topic,pickle.dumps(self.data)])
        except Exception as e:
            print(f"unable to send data with zmq: {e}")
            sys.exit(-1)

    def run(self):
        """
        The run function is the main method for receiving and sending data. Every 2 seconds
        all data is received, computed and later sent using ZeroMQ.
        The data is temporarily stored in a list (self.data)
        """
        logging.debug(f'entering endless loop')
        while True:
            self.data = self.gen_lidar_message()
            print(self.data)
            self.publish_data()
            self.data.clear()
            sleep(2) # Wait for 2 seconds.

    def gen_lidar_message(self):
        """
        This method generates values to simulate values from the ZX lidar.

        Return
        ------
        list
            data that contains all lidar data
            Shape: len = 84
        """
        
        # ToDo: Compute-generate hard-coded time stamp, vary measurement values by using random numbers (e.g. value + random_mumber or value * random_number)
        data =  [str(calendar.timegm(datetime.fromtimestamp(timestamp=time(), tz=timezone.utc).utctimetuple())), uptime.uptime(), 2027075.0, '2022-06-15 08:49:22+00:00',
                 200.0, 2027075.0, 1655282962, round(uniform(0,5),4), round(uniform(-2,2),4), round(uniform(180,200),4), 180.0, 2027076.0,
                 1655282963, round(uniform(0,5),4),round(uniform(-2,2),4), round(uniform(180,200),4), 160.0, 2027077.0, 1655282964, round(uniform(0,5),4), round(uniform(-2,2),4),round(uniform(180,200),4), 140.0, 2027078.0, 1655282965, round(uniform(0,5),4),round(uniform(-2,2),4),
                 round(uniform(180,200),4), 120.0, 2027079.0, 1655282966, round(uniform(0,5),4), round(uniform(-2,2),4), round(uniform(180,200),4), 100.0, 2027080.0, 1655282967, round(uniform(0,5),4), round(uniform(-2,2),4), round(uniform(180,200),4), 80.0, 2027081.0,
                 1655282968, round(uniform(0,5),4), round(uniform(-2,2),4),round(uniform(180,200),4), 60.0, 2027082.0, 1655282969, round(uniform(0,5),4), round(uniform(-2,2),4),round(uniform(180,200),4), 40.0, 2027083.0, 1655282970, round(uniform(0,5),4), round(uniform(-2,2),4),
                 round(uniform(180,200),4), 20.0, 2027084.0, 1655282971, round(uniform(0,5),4), round(uniform(-2,2),4),round(uniform(180,200),4), 0.0, 2027085.0, 1655282972, round(uniform(0,5),4), round(uniform(-2,2),4),round(uniform(180,200),4), 22.6, 12.28, 1021.6, 0.31,
                 0.0, 41.9, 0.0, 213.2, 38.0, 38.0, 23.0, 53.11, 8.86, 1.0]
        return data


if __name__ == "__main__":
    Fx = Lidar()
    Fx.bind_zmq_connection()
    Fx.run()
