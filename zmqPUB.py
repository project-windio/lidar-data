import zmq
import sys
import logging
from time import sleep
import pickle

def main():

    #connection = f'{config["ipc_protocol"]}:{config["ipc_port"]}'
    connection  = ('tcp://127.0.0.1:2000')
    logging.debug(f'binding to {connection} for zeroMQ IPC')
    context = zmq.Context()
    socket = context.socket(zmq.PUB)

    try:
        socket.bind(connection)

    except Exception as e:
        logging.fatal('failed to connect to zeroMQ socket for IPC')
        sys.exit(-1)

    logging.debug(f'connected to zeroMQ IPC socket')

    logging.debug(f'entering endless loop')

    #try:
    #    while True:
    #        with open("Windspeed.pickle", "rb") as pickle_file:
    #            lidar_data = pickle.load(pickle_file)
    #            print(lidar_data)
    #            lidar_data = socket.send_pyobj(lidar_data)
    #            sleep(1)


    try:
        with open("Windspeed.pickle", "rb") as pickle_file:
            lidar_data = pickle.load(pickle_file)
        reference = lidar_data["reference"]
        while True:
            with open("Windspeed.pickle", "rb") as pickle_file:
                lidar_data = pickle.load(pickle_file)
            referenceCur = lidar_data["reference"]
            if reference == referenceCur:
                sleep(1)
            else:
                socket.send_pyobj(lidar_data)
                reference = referenceCur

    except StopIteration:
        logging.fatal("GPSD has terminated")

    except KeyboardInterrupt:
        logging.info('goodbye')
        sys.exit(0)

connection = main()