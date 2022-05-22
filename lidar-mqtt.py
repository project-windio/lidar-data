import paho.mqtt.client as mqtt
from paho.mqtt.client import ssl as mqtt_ssl
import pytz, zmq, sys, logging, pickle, json
from datetime import datetime
from time import sleep

def lidar_dic_test( id="urn:uni-bremen:bik:wio:0:0:wnds:0002", temperature = None):
    data = {
        "content-spec": "urn:spec://eclipse.org/unide/measurement-message#v3",
        "device": {
            "id": id
        },
        "measurements": [
            {
                "context": {
                    "temperature": {
                        "unit": "celsius"
                    }
                },
                "ts": "2022-06-16 11:08:25+00:00",
                "series": {
                    "time": [
                        0
                    ],
                    "temperature": [
                        temperature
                    ],
                }
            }
        ]
    }
    data_json = json.dumps(data, indent=4)
    return data_json


def lidar_dic(id="urn:uni-bremen:bik:wio:0:0:wnds:0002", temperature = None, battery = None,
              airPressure = None, ground_windspeed = None, tilt = None, humidity = None, horinzontal_windspeed_height_1 = None, horinzontal_windspeed_height_2 = None,
              horinzontal_windspeed_height_3 = None, horinzontal_windspeed_height_4 = None, horinzontal_windspeed_height_5 = None, horinzontal_windspeed_height_6 = None,
              horinzontal_windspeed_height_7 = None, horinzontal_windspeed_height_8 = None, horinzontal_windspeed_height_9 = None,
              horinzontal_windspeed_height_10 = None, horinzontal_windspeed_height_11 = None, set_height_1 = None, set_height_2 = None, set_height_3 = None, set_height_4 = None,
              set_height_5 = None, set_height_6 = None, set_height_7 = None, set_height_8 = None, set_height_9 = None, set_height_10 = None, set_height_11 = None,
              time_stamp_data_received = None, reference=None, time_stamp_data_generated= None):
    #creating a .json structure to be able to use the data
    data =  {
    "content-spec": "urn:spec://eclipse.org/unide/measurement-message#v3",
    "device": {
        "id": id
    },
    "measurements": [
        {
        "context": {
                "temperature":{
                    "unit": "celsius"
                },
                "battery":{
                    "unit": "volt"
                },
                "airPressure":{
                    "unit": "hPa"
                },
                "ground_windspeed":{
                    "unit": "m/s"
                },
                "tilt":{
                    "unit": "degr."
                },
                "humidity":{
                    "unit": "perc."
                },

                "horinzontal_windspeed_height_1": {
                    "unit": "m/s"
                },
                "horinzontal_windspeed_height_2": {
                    "unit": "m/s"
                },
                "horinzontal_windspeed_height_3": {
                    "unit": "m/s"
                },
                "horinzontal_windspeed_height_4": {
                    "unit": "m/s"
                },
                "horinzontal_windspeed_height_5": {
                    "unit": "m/s"
                },
                "horinzontal_windspeed_height_6": {
                    "unit": "m/s"
                },
                "horinzontal_windspeed_height_7": {
                    "unit": "m/s"
                },
                "horinzontal_windspeed_height_8": {
                    "unit": "m/s"
                },
                "horinzontal_windspeed_height_9": {
                    "unit": "m/s"
                },
                "horinzontal_windspeed_height_10": {
                    "unit": "m/s"
                },
                "horinzontal_windspeed_height_11": {
                    "unit": "m/s"
                },
                "set_height_1":{
                    "unit": "m"
                },
                "set_height_2": {
                    "unit": "m"
                },
                "set_height_3": {
                    "unit": "m"
                },
                "set_height_4": {
                    "unit": "m"
                },
                "set_height_5": {
                    "unit": "m"
                },
                "set_height_6": {
                    "unit": "m"
                },
                "set_height_7": {
                    "unit": "m"
                },
                "set_height_8": {
                    "unit": "m"
                },
                "set_height_9": {
                    "unit": "m"
                },
                "set_height_10": {
                    "unit": "m"
                },
                "set_height_11": {
                    "unit": "m"
                },
                "time_stamp_data_received":{
                    "unit": "Unix"
                },
                "reference":{
                    "unit": "index"
                }
            },
                #"ts": "2022-06-16 11:08:28+00:00",
                "ts": time_stamp_data_generated,
                "series":{
                    "time": [
                    0
                    ],
                    "temperature": [
                        temperature
                    ],
                    "battery": [
                        battery
                    ],
                    "airPressure": [
                        airPressure
                    ],
                    "ground_windspeed": [
                        ground_windspeed
                    ],
                    "tilt": [
                        tilt
                    ],
                    "humidity": [
                        humidity
                    ],
                    "horinzontal_windspeed_height_1": [
                        horinzontal_windspeed_height_1
                    ],
                    "horinzontal_windspeed_height_2": [
                        horinzontal_windspeed_height_2
                    ],
                    "horinzontal_windspeed_height_3": [
                        horinzontal_windspeed_height_3
                    ],
                    "horinzontal_windspeed_height_4": [
                        horinzontal_windspeed_height_4
                    ],
                    "horinzontal_windspeed_height_5": [
                        horinzontal_windspeed_height_5
                    ],
                    "horinzontal_windspeed_height_6": [
                        horinzontal_windspeed_height_6
                    ],
                    "horinzontal_windspeed_height_7": [
                        horinzontal_windspeed_height_7
                    ],
                    "horinzontal_windspeed_height_8": [
                        horinzontal_windspeed_height_8
                    ],
                    "horinzontal_windspeed_height_9": [
                        horinzontal_windspeed_height_9
                    ],
                    "horinzontal_windspeed_height_10": [
                        horinzontal_windspeed_height_10
                    ],
                    "horinzontal_windspeed_height_11": [
                        horinzontal_windspeed_height_11
                    ],
                    "set_height_1":[
                        set_height_1
                    ],
                    "set_height_2":[
                        set_height_2
                    ],
                    "set_height_3": [
                        set_height_3
                    ],
                    "set_height_4": [
                        set_height_4
                    ],
                    "set_height_5": [
                        set_height_5
                    ],
                    "set_height_6": [
                        set_height_6
                    ],
                    "set_height_7": [
                        set_height_7
                    ],
                    "set_height_8": [
                        set_height_8
                    ],
                    "set_height_9": [
                        set_height_9
                    ],
                    "set_height_10": [
                        set_height_10
                    ],
                    "set_height_11": [
                        set_height_11
                    ],
                    "time_stamp_data_received": [
                        time_stamp_data_received
                    ],
                    "reference": [
                        reference
                    ]
                }
            }
        ]
    }
    data_json = json.dumps(data, indent=4)
    return data_json


with open('lidar-mqtt.json') as json_file:
    config = json.load(json_file)
    print(config)
    user = config['user']
    password = config['password']
    url = config['url']
    port = config['port']
    edge_id = config['edge_id']
    device_id = config['device_id']
    mqtt_topic = "ppmpv3/3/DDATA/" + edge_id + "/" + device_id

client = mqtt.Client()
print("Working with user: " + user)
client.username_pw_set(user, password)
client.connect(url, port)
print("Successfully connected.")
client.tls_set_context(mqtt_ssl.create_default_context())
client.loop_start()

#zmq Subscriber


data = lidar_dic()
context = zmq.Context()
socket = context.socket(zmq.SUB)
try:
    #connecting to the same ip-adress and port as the publisher
    socket.connect('tcp://127.0.0.1:5557')
    #socket.bind('tcp://127.0.0.1:2000')
except Exception as e :
    logging.FATAL(f'Failed to bind to zeromq socket:{e}')
    sys.exit(-1)

socket.setsockopt_string(zmq.SUBSCRIBE, '')
print('Successfully bound to zeroMQ receiver socket as subscriber')
first_message = True
print('Trying to receive data.')
try:
    while True:
        message = socket.recv_pyobj() #receives data and ZMQ-topic
        zmq_topic = message[0].decode('utf-8')

        if first_message:
            print(f'Received first data: {message}')
            print(f'From topic: {zmq_topic}')
            first_message = False

        if zmq_topic == "ldr": #filling the .json structure with the data published by zmqPUB.py
            data = lidar_dic(
                             temperature=message[1]["met_station"]["temperature"],
                             battery=message[1]["met_station"]["battery"],
                             airPressure=message[1]["met_station"]["airPressure"],
                             ground_windspeed=message[1]["met_station"]["windspeed"],
                             tilt=message[1]["met_station"]["tilt"],
                             humidity=message[1]["met_station"]["humidity"],
                             horinzontal_windspeed_height_1=message[1]["horizontal_windspeed"]["height_1"],
                             horinzontal_windspeed_height_2=message[1]["horizontal_windspeed"]["height_2"],
                             horinzontal_windspeed_height_3=message[1]["horizontal_windspeed"]["height_3"],
                             horinzontal_windspeed_height_4=message[1]["horizontal_windspeed"]["height_4"],
                             horinzontal_windspeed_height_5=message[1]["horizontal_windspeed"]["height_5"],
                             horinzontal_windspeed_height_6=message[1]["horizontal_windspeed"]["height_6"],
                             horinzontal_windspeed_height_7=message[1]["horizontal_windspeed"]["height_7"],
                             horinzontal_windspeed_height_8=message[1]["horizontal_windspeed"]["height_8"],
                             horinzontal_windspeed_height_9=message[1]["horizontal_windspeed"]["height_9"],
                             horinzontal_windspeed_height_10=message[1]["horizontal_windspeed"]["height_10"],
                             horinzontal_windspeed_height_11=message[1]["horizontal_windspeed"]["height_11"],
                             set_height_1=message[1]["set_heights"]["height_1"],
                             set_height_2=message[1]["set_heights"]["height_2"],
                             set_height_3=message[1]["set_heights"]["height_3"],
                             set_height_4=message[1]["set_heights"]["height_4"],
                             set_height_5=message[1]["set_heights"]["height_5"],
                             set_height_6=message[1]["set_heights"]["height_6"],
                             set_height_7=message[1]["set_heights"]["height_7"],
                             set_height_8=message[1]["set_heights"]["height_8"],
                             set_height_9=message[1]["set_heights"]["height_9"],
                             set_height_10=message[1]["set_heights"]["height_10"],
                             set_height_11=message[1]["set_heights"]["height_11"],
                             time_stamp_data_received=message[1]["time_stamp_data_received"],
                             reference=message[1]["reference"],
                             time_stamp_data_generated=str(message[1]["time_stamp_data_generated"]))
            print(data)
            client.publish(mqtt_topic,data)
        else:
            print(f'Only use topic "lidar" is used, however I received data on topic: {zmq_topic}')
        sleep(1)
        continue

except KeyboardInterrupt:
    client.loop_stop()
    print('Interrupted!')
    pass


"""

with open('lidar-mqtt.json') as json_file:
    config = json.load(json_file)
    print(config)
    user = config['user']
    password = config['password']
    url = config['url']
    port = config['port']
    edge_id = config['edge_id']
    device_id = config['device_id']
    mqtt_topic = "ppmpv3/3/DDATA/" + edge_id + "/" + device_id

client = mqtt.Client()
print("Working with user: " + user)
client.username_pw_set(user, password)
client.connect(url, port)
print("Successfully connected.")
client.tls_set_context(mqtt_ssl.create_default_context())
client.loop_start()

#zmq Subscriber


data = lidar_dic()
context = zmq.Context()
socket = context.socket(zmq.SUB)
try:
    #connecting to the same ip-adress and port as the publisher
    socket.connect('tcp://127.0.0.1:5557')
    #socket.bind('tcp://127.0.0.1:2000')
except Exception as e :
    logging.FATAL(f'Failed to bind to zeromq socket:{e}')
    sys.exit(-1)

socket.setsockopt_string(zmq.SUBSCRIBE, '')
print('Successfully bound to zeroMQ receiver socket as subscriber')
first_message = True
print('Trying to receive data.')
try:
    while True:
        message = socket.recv_pyobj() #receives data and ZMQ-topic
        zmq_topic = message[0].decode('utf-8')

        if first_message:
            print(f'Received first data: {message}')
            print(f'From topic: {zmq_topic}')
            first_message = False

        if zmq_topic == "ldr": #filling the .json structure with the data published by zmqPUB.py
            data = lidar_dic_test(
                             temperature=message[1]["met_station"]["temperature"],
                             )
            print(data)
            client.publish(mqtt_topic,data)
        else:
            print(f'Only use topic "lidar" is used, however I received data on topic: {zmq_topic}')
        sleep(1)
        continue

except KeyboardInterrupt:
    client.loop_stop()
    print('Interrupted!')
    pass
"""

def test():

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    try:
        socket.connect('tcp://127.0.0.1:5557')
    except Exception as e:
        logging.fatal(f'Failed to bind to zeromq socket:{e}')
        sys.exit(-1)

    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    print('Successfully bound to zeroMQ receiver socket as subscriber')
    first_message = False
    print('Trying to receive data.')
    try:
        while True:
            message = socket.recv_pyobj()
            zmq_topic = message[0].decode('utf-8')

            if first_message:
                print(f'Received first data: {message}')
                print(f'From topic: {zmq_topic}')
                first_message = False

            if zmq_topic == "ldr":

                data = lidar_dic(time_stamp_data_generated=str(message[1]["time_stamp_data_generated"]),
                                 time_stamp_data_received=message[1]["time_stamp_data_received"],
                                 reference_generated=message[1]["reference"],
                                 temperature=message[1]["met_station"]["temperature"],
                                 battery=message[1]["met_station"]["battery"],
                                 airPressure=message[1]["met_station"]["airPressure"],
                                 ground_windspeed=message[1]["met_station"]["windspeed"],
                                 tilt=message[1]["met_station"]["tilt"],
                                 humidity=message[1]["met_station"]["humidity"],
                                 horinzontal_windspeed_height_1=message[1]["horizontal_windspeed"]["height_1"],
                                 horinzontal_windspeed_height_2=message[1]["horizontal_windspeed"]["height_2"], 
                                 horinzontal_windspeed_height_3=message[1]["horizontal_windspeed"]["height_3"], 
                                 horinzontal_windspeed_height_4=message[1]["horizontal_windspeed"]["height_4"],
                                 horinzontal_windspeed_height_5=message[1]["horizontal_windspeed"]["height_5"],
                                 horinzontal_windspeed_height_6=message[1]["horizontal_windspeed"]["height_6"], 
                                 horinzontal_windspeed_height_7=message[1]["horizontal_windspeed"]["height_7"],
                                 horinzontal_windspeed_height_8=message[1]["horizontal_windspeed"]["height_8"], 
                                 horinzontal_windspeed_height_9=message[1]["horizontal_windspeed"]["height_9"], 
                                 horinzontal_windspeed_height_10=message[1]["horizontal_windspeed"]["height_10"],
                                 horinzontal_windspeed_height_11=message[1]["horizontal_windspeed"]["height_11"],
                                 set_height_1=message[1]["set_heights"]["height_1"],
                                 set_height_2=message[1]["set_heights"]["height_2"],
                                 set_height_3=message[1]["set_heights"]["height_3"],
                                 set_height_4=message[1]["set_heights"]["height_4"],
                                 set_height_5=message[1]["set_heights"]["height_5"],
                                 set_height_6=message[1]["set_heights"]["height_6"],
                                 set_height_7=message[1]["set_heights"]["height_7"],
                                 set_height_8=message[1]["set_heights"]["height_8"],
                                 set_height_9=message[1]["set_heights"]["height_9"],
                                 set_height_10=message[1]["set_heights"]["height_10"],
                                 set_height_11=message[1]["set_heights"]["height_11"])
                                 
                                 

                """
                data = lidar_dic(time_stamp_data_generated=str(message[1]["time_stamp_data_generated"]), time_stamp_data_received=message[1]["time_stamp_data_received"], reference_generated=message[1]["reference"],
                                 temperature=message[1]["Met_station"]["temperature"], battery=message[1]["Met_station"]["battery"], airPressure=message[1]["Met_station"]["airPressure"], ground_windspeed=message[1]["Met_station"]["windspeed"],
                                 tilt=message[1]["Met_station"]["tilt"], humidity=message[1]["Met_station"]["humidity"], horinzontal_windspeed_height_1=message[1]["horizontal_windspeed"]["height1"],
                                 horinzontal_windspeed_height_2=message[1]["horizontal_windspeed"]["height2"], horinzontal_windspeed_height_3=message[1]["horizontal_windspeed"]["height3"], horinzontal_windspeed_height_4=message[1]["horizontal_windspeed"]["height4"],
                                 horinzontal_windspeed_height_5=message[1]["horizontal_windspeed"]["height5"],horinzontal_windspeed_height_6=message[1]["horizontal_windspeed"]["height6"], horinzontal_windspeed_height_7=message[1]["horizontal_windspeed"]["height7"],
                                 horinzontal_windspeed_height_8=message[1]["horizontal_windspeed"]["height8"], horinzontal_windspeed_height_9=message[1]["horizontal_windspeed"]["height9"], horinzontal_windspeed_height_10=message[1]["horizontal_windspeed"]["height10"],
                                 horinzontal_windspeed_height_11=message[1]["horizontal_windspeed"]["height11"],
                                 set_heights_1=102,
                                 set_heights_2=102,
                                 set_heights_3=90,
                                 set_heights_4=83,
                                 set_heights_5=54,
                                 set_heights_6=83,
                                 set_heights_7=92,
                                 set_heights_8=43,
                                 set_heights_9=78,
                                 set_heights_10=14,
                                 set_heights_11=6007)
                """


                with open("lidar_data.json", "w") as write_file:
                    json.dump(data, write_file)

                with open("lidar_data.json", "r") as read_file:
                    print(json.load(read_file))


                client.publish(mqtt_topic, data_json)


            else:
                print(f'Only use topic "lidar" is used, however I received data on topic: {zmq_topic}')
            sleep(10)



            continue

    except KeyboardInterrupt:
        client.loop_stop()
        print('Interrupted!')
        pass


#beispiel = mqtt()
