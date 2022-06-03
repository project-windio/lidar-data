import os.path

import zmq
import sys
import logging
from time import sleep
import pickle

def main():
    #setting the Ip-adress and port number for the connection
    #connection = f'{config["ipc_protocol"]}:{config["ipc_port"]}'
    connection  = ('tcp://127.0.0.1:5557')
    logging.debug(f'binding to {connection} for zeroMQ IPC')
    context = zmq.Context()
    socket = context.socket(zmq.PUB)

    try:
        socket.bind(connection) #establishing the connection

    except Exception as e:
        logging.fatal('failed to connect to zeroMQ socket for IPC')
        sys.exit(-1)

    logging.debug(f'connected to zeroMQ IPC socket')

    logging.debug(f'entering endless loop')
    LIDAR_TOPIC = "ldr".encode('utf-8') #setting the Topic for ZMQ

    #try:
    #    while True:
    #        with open("Windspeed.pickle", "rb") as pickle_file:
    #            lidar_data = pickle.load(pickle_file)
    #            print(lidar_data)
    #            lidar_data = socket.send_pyobj([LIDAR_TOPIC,lidar_data])
    #            #return False
    #            sleep(15)


    try:
        with open("Windspeed.pickle", "rb") as pickle_file:
            lidar_data = pickle.load(pickle_file)
        reference = lidar_data["reference"]
        while True:
            if os.path.getsize("Windspeed.pickle") > 0:
             #opening the pickle file and checking whether a new data set is available
                with open("Windspeed.pickle", "rb") as pickle_file:
                    lidar_data = pickle.load(pickle_file)
                referenceCur = lidar_data["reference"]
                if reference == referenceCur:
                    sleep(1)
                else:
                  #if a new data set is available the topic and data is send via ZMQ
                    lidar_data = socket.send_pyobj([LIDAR_TOPIC,lidar_data])
                    reference = referenceCur
            else:
                sleep(0.2)

    except StopIteration:
        logging.fatal("GPSD has terminated")

    except KeyboardInterrupt:
        logging.info('goodbye')
        sys.exit(0)


connection = main()