import paho.mqtt.client as mqtt
from paho.mqtt.client import ssl as mqtt_ssl
import zmq, sys, logging, json
from time import sleep

def lidar_dic_test(id="urn:uni-bremen:bik:wio:0:0:wnds:0002", temperature = None, battery = None,
              airPressure = None, ground_windspeed = None, tilt = None, humidity = None, raining = None, met_wind_direction = None,pod_upper_temperature = None,
              pod_lower_temperature = None,pod_humidity = None,gps_latitude = None, gps_longitude = None,  timestamp_data_received = None, reference=None, timestamp_data_generated= None, scan_dwell_time= None):
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
                "raining":{
                    "unit": "-"
                },
                "met_wind_direction":{
                    "unit": "degr."
                },
                "pod_upper_temperature": {
                    "unit": "degr."
                },
                "pod_lower_temperature": {
                    "unit": "degr."
                },
                "pod_humidity": {
                    "unit": "perc."
                },
                "gps_latitude": {
                    "unit": "degr."
                },
                "gps_longitude": {
                    "unit": "degr."
                },
                "scan_dwell_time":{
                    "unit": "s"
                },
                "timestamp_data_received":{
                    "unit": "Unix"
                },
                "reference":{
                    "unit": "index"
                }
            },
                #"ts": "2022-06-16 11:08:28+00:00",
                "ts": timestamp_data_generated,
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
                    "raining": [
                        raining
                    ],
                    "met_wind_direction": [
                        met_wind_direction
                    ],
                    "pod_upper_temperature": [
                        pod_upper_temperature
                    ],
                    "pod_lower_temperature": [
                        pod_lower_temperature
                    ],
                    "pod_humidity": [
                        pod_humidity
                    ],
                    "gps_latitude": [
                        gps_latitude
                    ],
                    "gps_longitude": [
                        gps_longitude
                    ],
                    "scan_dwell_time":[
                        scan_dwell_time
                    ],
                    "timestamp_data_received": [
                        timestamp_data_received
                    ],
                    "reference": [
                        reference
                    ]
                }
            }
        ]
    }


    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    try:
        # connecting to the same ip-adress and port as the publisher
        socket.connect('tcp://127.0.0.1:5557')
    except Exception as e:
        logging.FATAL(f'Failed to bind to zeromq socket:{e}')
        sys.exit(-1)

    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    try:
        message = socket.recv_pyobj()  # receives data and ZMQ-topic
        zmq_topic = message[0].decode('utf-8')

        if zmq_topic == "ldr":  # filling the .json structure with the data published by zmqPUB.py
            for i in range(len((message[1]["horizontal_windspeed"]))):
                data["measurements"][0]["context"]["set_height_" + str(i + 1)] = {"unit": "m"}
                data["measurements"][0]["context"]["horinzontal_windspeed_height_" + str(i+1)] = {"unit": "m/s"}
                data["measurements"][0]["context"]["individual_timestamp_" + str(i + 1)] = {"unit": "s"}
                data["measurements"][0]["series"]["set_height_" + str(i + 1)] = [message[1]["set_heights"]["height_" + str(i+1)]]
                data["measurements"][0]["series"]["horinzontal_windspeed_height_" + str(i + 1)] = [message[1]["horizontal_windspeed"]["height_" + str(i+1)]]
                data["measurements"][0]["series"]["individual_timestamp_" + str(i + 1)] = [message[1]["individual_timestamp"]["height_" + str(i+1)]]
            data_json = json.dumps(data, indent=4)
        else:
            print(f'Only use topic "ldr" is used, however I received data on topic: {zmq_topic}')

    except KeyboardInterrupt:
        client.loop_stop()
        print('Interrupted!')
        pass
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

data = lidar_dic_test()
context = zmq.Context()
socket = context.socket(zmq.SUB)
try:
    #connecting to the same ip-adress and port as the publisher
    socket.connect('tcp://127.0.0.1:5557')
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
                             battery=message[1]["met_station"]["battery"],
                             airPressure=message[1]["met_station"]["airPressure"],
                             ground_windspeed=message[1]["met_station"]["windspeed"],
                             tilt=message[1]["met_station"]["tilt"],
                             humidity=message[1]["met_station"]["humidity"],
                             raining=message[1]["met_station"]["raining"],
                             met_wind_direction=message[1]["met_station"]["met_wind_direction"],
                             pod_upper_temperature=message[1]["met_station"]["pod_upper_temperature"],
                             pod_lower_temperature=message[1]["met_station"]["pod_lower_temperature"],
                             pod_humidity=message[1]["met_station"]["pod_humidity"],
                             gps_latitude=message[1]["met_station"]["gps_latitude"],
                             gps_longitude=message[1]["met_station"]["gps_longitude"],
                             timestamp_data_received=message[1]["timestamp_data_received"],
                             reference=message[1]["reference"],
                             timestamp_data_generated=str(message[1]["timestamp_data_generated"]),
                             scan_dwell_time=message[1]["met_station"]["scan_dwell_time"])
            print(data)
            client.publish(mqtt_topic,data)
        else:
            print(f'Only use topic "ldr" is used, however I received data on topic: {zmq_topic}')
        sleep(1)
        continue

except KeyboardInterrupt:
    client.loop_stop()
    print('Interrupted!')
    pass
