import paho.mqtt.client as mqtt
from paho.mqtt.client import ssl as mqtt_ssl
import pytz, zmq, sys, logging, pickle, json
from datetime import datetime


def create_mqtt_payload(id="urn:uni-bremen:bik:wio:0:0:wnds:0002", acc_x=0, acc_y=0, acc_z=0,set_height_1=None,set_height_2=None,set_height_3=None,
                        set_height_4=None,set_height_5=None,set_height_6=None,set_height_7=None,set_height_8=None,set_height_9=None,set_height_10=None,set_height_11=None,
                        individual_reference_1=None,individual_reference_2=None,individual_reference_3=None,individual_reference_4=None,individual_reference_5=None,
                        individual_reference_6=None,individual_reference_7=None,individual_reference_8=None,individual_reference_9=None,individual_reference_10=None,individual_reference_11=None,
                        individual_timestamp_1=None,individual_timestamp_2=None,individual_timestamp_3=None,individual_timestamp_4=None,individual_timestamp_5=None,individual_timestamp_6=None,
                        individual_timestamp_7=None,individual_timestamp_8=None,individual_timestamp_9=None,individual_timestamp_10=None,individual_timestamp_11=None,
                        horinzontal_windspeed_height_1=None,horinzontal_windspeed_height_2=None,horinzontal_windspeed_height_3=None,horinzontal_windspeed_height_4=None,horinzontal_windspeed_height_5=None,horinzontal_windspeed_height_6=None,
                        horinzontal_windspeed_height_7=None,horinzontal_windspeed_height_8=None,horinzontal_windspeed_height_9=None,horinzontal_windspeed_height_10=None,horinzontal_windspeed_height_11=None,
                        vertical_windspeed_height_1=None,vertical_windspeed_height_2=None,vertical_windspeed_height_3=None,vertical_windspeed_height_4=None,vertical_windspeed_height_5=None,vertical_windspeed_height_6=None,
                        vertical_windspeed_height_7=None,vertical_windspeed_height_8=None,vertical_windspeed_height_9=None,vertical_windspeed_height_10=None,vertical_windspeed_height_11=None,
                        wind_direction_1=None,wind_direction_2=None,wind_direction_3=None,wind_direction_4=None,wind_direction_5=None,wind_direction_6=None,wind_direction_7=None,
                        wind_direction_8=None,wind_direction_9=None,wind_direction_10=None,wind_direction_11=None,temperature = None, battery = None,
                        airPressure = None, ground_windspeed = None, tilt = None, humidity = None, raining = None, met_wind_direction = None, pod_upper_temperature = None,
                        pod_lower_temperature = None, pod_humidity = None, gps_latitude = None, gps_longitude = None, timestamp_data_received = None, reference=None, timestamp_data_generated= None, scan_dwell_time= None):

    """
    Creates a WindIO MQTT payload based on a motion sensor box log file line.
    Example for payload from WindIO documentation:
    {
    "content-spec": "urn:spec://eclipse.org/unide/measurement-message#v3",
    "device": {
        "id": "urn:uni-bremen:bik:wio:1:1:wind:1234"
    },
    "measurements": [
        {
        "context": {
            "temperature":  {
            "unit": "Cel"
            }
        },
        "ts": "2021-05-18T07:43:16.969Z",
        "series": {
            "time": [
            0
            ],
            "temperature": [
            35.4231
            ]
        }
        }
    ]
    }
    """
    dict = {
        "content-spec": "urn:spec://eclipse.org/unide/measurement-message#v3",
        "device": {
            "id": id
        },
        "measurements": [
            {
                "context": {
                    "timestamp_data_received": {
                        "unit": "Unix"
                    },
                    "set_height_1":{
                        "unit":"m"
                    },
                    "individual_reference_1":{
                        "unit":"index"
                    },
                    "individual_timestamp_1":{
                        "unit":"Unix"
                    },
                    "horinzontal_windspeed_height_1":{
                        "unit":"m/s"
                    },
                    "vertical_windspeed_height_1":{
                        "unit":"m/s"
                    },
                    "wind_direction_1":{
                        "unit":"degr."
                    },
                    "set_height_2": {
                        "unit": "m"
                    },
                    "individual_reference_2": {
                        "unit": "index"
                    },
                    "individual_timestamp_2": {
                        "unit": "Unix"
                    },
                    "horinzontal_windspeed_height_2": {
                        "unit": "m/s"
                    },
                    "vertical_windspeed_height_2": {
                        "unit": "m/s"
                    },
                    "wind_direction_2": {
                        "unit": "degr."
                    },
                    "set_height_3": {
                        "unit": "m"
                    },
                    "individual_reference_3": {
                        "unit": "index"
                    },
                    "individual_timestamp_3": {
                        "unit": "Unix"
                    },
                    "horinzontal_windspeed_height_3": {
                        "unit": "m/s"
                    },
                    "vertical_windspeed_height_3": {
                        "unit": "m/s"
                    },
                    "wind_direction_3": {
                        "unit": "degr."
                    },
                    "set_height_4": {
                        "unit": "m"
                    },
                    "individual_reference_4": {
                        "unit": "index"
                    },
                    "individual_timestamp_4": {
                        "unit": "Unix"
                    },
                    "horinzontal_windspeed_height_4": {
                        "unit": "m/s"
                    },
                    "vertical_windspeed_height_4": {
                        "unit": "m/s"
                    },
                    "wind_direction_4": {
                        "unit": "degr."
                    },
                    "set_height_5": {
                        "unit": "m"
                    },
                    "individual_reference_5": {
                        "unit": "index"
                    },
                    "individual_timestamp_5": {
                        "unit": "Unix"
                    },
                    "horinzontal_windspeed_height_5": {
                        "unit": "m/s"
                    },
                    "vertical_windspeed_height_5": {
                        "unit": "m/s"
                    },
                    "wind_direction_5": {
                        "unit": "degr."
                    },
                    "set_height_6": {
                        "unit": "m"
                    },
                    "individual_reference_6": {
                        "unit": "index"
                    },
                    "individual_timestamp_6": {
                        "unit": "Unix"
                    },
                    "horinzontal_windspeed_height_6": {
                        "unit": "m/s"
                    },
                    "vertical_windspeed_height_6": {
                        "unit": "m/s"
                    },
                    "wind_direction_6": {
                        "unit": "degr."
                    },
                    "set_height_7": {
                        "unit": "m"
                    },
                    "individual_reference_7": {
                        "unit": "index"
                    },
                    "individual_timestamp_7": {
                        "unit": "Unix"
                    },
                    "horinzontal_windspeed_height_7": {
                        "unit": "m/s"
                    },
                    "vertical_windspeed_height_7": {
                        "unit": "m/s"
                    },
                    "wind_direction_7": {
                        "unit": "degr."
                    },
                    "set_height_8": {
                        "unit": "m"
                    },
                    "individual_reference_8": {
                        "unit": "index"
                    },
                    "individual_timestamp_8": {
                        "unit": "Unix"
                    },
                    "horinzontal_windspeed_height_8": {
                        "unit": "m/s"
                    },
                    "vertical_windspeed_height_8": {
                        "unit": "m/s"
                    },
                    "wind_direction_8": {
                        "unit": "degr."
                    },
                    "set_height_9": {
                        "unit": "m"
                    },
                    "individual_reference_9": {
                        "unit": "index"
                    },
                    "individual_timestamp_9": {
                        "unit": "Unix"
                    },
                    "horinzontal_windspeed_height_9": {
                        "unit": "m/s"
                    },
                    "vertical_windspeed_height_9": {
                        "unit": "m/s"
                    },
                    "wind_direction_9": {
                        "unit": "degr."
                    },
                    "set_height_10": {
                        "unit": "m"
                    },
                    "individual_reference_10": {
                        "unit": "index"
                    },
                    "individual_timestamp_10": {
                        "unit": "Unix"
                    },
                    "horinzontal_windspeed_height_10": {
                        "unit": "m/s"
                    },
                    "vertical_windspeed_height_10": {
                        "unit": "m/s"
                    },
                    "wind_direction_10": {
                        "unit": "degr."
                    },
                    "set_height_11": {
                        "unit": "m"
                    },
                    "individual_reference_11": {
                        "unit": "index"
                    },
                    "individual_timestamp_11": {
                        "unit": "Unix"
                    },
                    "horinzontal_windspeed_height_11": {
                        "unit": "m/s"
                    },
                    "vertical_windspeed_height_11": {
                        "unit": "m/s"
                    },
                    "wind_direction_11": {
                        "unit": "degr."
                    },
                    "reference": {
                        "unit": "index"
                    },
                    "temperature": {
                        "unit": "celsius"
                    },
                    "battery": {
                        "unit": "volt"
                    },
                    "airPressure": {
                        "unit": "hPa"
                    },
                    "ground_windspeed": {
                        "unit": "m/s"
                    },
                    "tilt": {
                        "unit": "degr."
                    },
                    "humidity": {
                        "unit": "perc."
                    },
                    "raining": {
                        "unit": "-"
                    },
                    "met_wind_direction": {
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
                    "scan_dwell_time": {
                        "unit": "s"
                    }
                },
                "ts": timestamp_data_generated,
                "series": {
                    "time": [
                        0
                    ],
                    "timestamp_data_received": [
                        timestamp_data_received
                    ],
                    "set_height_1":[
                        set_height_1
                    ],
                    "individual_reference_1":[
                        individual_reference_1
                    ],
                    "individual_timestamp_1":[
                        individual_timestamp_1
                    ],
                    "horinzontal_windspeed_height_1":[
                        horinzontal_windspeed_height_1
                    ],
                    "vertical_windspeed_height_1":[
                        vertical_windspeed_height_1
                    ],
                    "wind_direction_1":[
                        wind_direction_1
                    ],
                    "set_height_2": [
                        set_height_2
                    ],
                    "individual_reference_2": [
                        individual_reference_2
                    ],
                    "individual_timestamp_2": [
                        individual_timestamp_2
                    ],
                    "horinzontal_windspeed_height_2": [
                        horinzontal_windspeed_height_2
                    ],
                    "vertical_windspeed_height_2": [
                        vertical_windspeed_height_2
                    ],
                    "wind_direction_2": [
                        wind_direction_2
                    ],
                    "set_height_3": [
                        set_height_3
                    ],
                    "individual_reference_3": [
                        individual_reference_3
                    ],
                    "individual_timestamp_3": [
                        individual_timestamp_3
                    ],
                    "horinzontal_windspeed_height_3": [
                        horinzontal_windspeed_height_3
                    ],
                    "vertical_windspeed_height_3": [
                        vertical_windspeed_height_3
                    ],
                    "wind_direction_3": [
                        wind_direction_3
                    ],
                    "set_height_4": [
                        set_height_4
                    ],
                    "individual_reference_4": [
                        individual_reference_4
                    ],
                    "individual_timestamp_4": [
                        individual_timestamp_4
                    ],
                    "horinzontal_windspeed_height_4": [
                        horinzontal_windspeed_height_4
                    ],
                    "vertical_windspeed_height_4": [
                        vertical_windspeed_height_4
                    ],
                    "wind_direction_4": [
                        wind_direction_4
                    ],
                    "set_height_5": [
                        set_height_5
                    ],
                    "individual_reference_5": [
                        individual_reference_5
                    ],
                    "individual_timestamp_5": [
                        individual_timestamp_5
                    ],
                    "horinzontal_windspeed_height_5": [
                        horinzontal_windspeed_height_5
                    ],
                    "vertical_windspeed_height_5": [
                        vertical_windspeed_height_5
                    ],
                    "wind_direction_5": [
                        wind_direction_5
                    ],
                    "set_height_6": [
                        set_height_6
                    ],
                    "individual_reference_6": [
                        individual_reference_6
                    ],
                    "individual_timestamp_6": [
                        individual_timestamp_6
                    ],
                    "horinzontal_windspeed_height_6": [
                        horinzontal_windspeed_height_6
                    ],
                    "vertical_windspeed_height_6": [
                        vertical_windspeed_height_6
                    ],
                    "wind_direction_6": [
                        wind_direction_6
                    ],
                    "set_height_7": [
                        set_height_7
                    ],
                    "individual_reference_7": [
                        individual_reference_7
                    ],
                    "individual_timestamp_7": [
                        individual_timestamp_7
                    ],
                    "horinzontal_windspeed_height_7": [
                        horinzontal_windspeed_height_7
                    ],
                    "vertical_windspeed_height_7": [
                        vertical_windspeed_height_7
                    ],
                    "wind_direction_7": [
                        wind_direction_7
                    ],
                    "set_height_8": [
                        set_height_8
                    ],
                    "individual_reference_8": [
                        individual_reference_8
                    ],
                    "individual_timestamp_8": [
                        individual_timestamp_8
                    ],
                    "horinzontal_windspeed_height_8": [
                        horinzontal_windspeed_height_8
                    ],
                    "vertical_windspeed_height_8": [
                        vertical_windspeed_height_8
                    ],
                    "wind_direction_8": [
                        wind_direction_8
                    ],
                    "set_height_9": [
                        set_height_9
                    ],
                    "individual_reference_9": [
                        individual_reference_9
                    ],
                    "individual_timestamp_9": [
                        individual_timestamp_9
                    ],
                    "horinzontal_windspeed_height_9": [
                        horinzontal_windspeed_height_9
                    ],
                    "vertical_windspeed_height_9": [
                        vertical_windspeed_height_9
                    ],
                    "wind_direction_9": [
                        wind_direction_9
                    ],
                    "set_height_10": [
                        set_height_10
                    ],
                    "individual_reference_10": [
                        individual_reference_10
                    ],
                    "individual_timestamp_10": [
                        individual_timestamp_10
                    ],
                    "horinzontal_windspeed_height_10": [
                        horinzontal_windspeed_height_10
                    ],
                    "vertical_windspeed_height_10": [
                        vertical_windspeed_height_10
                    ],
                    "wind_direction_10": [
                        wind_direction_10
                    ],
                    "set_height_11": [
                        set_height_11
                    ],
                    "individual_reference_11": [
                        individual_reference_11
                    ],
                    "individual_timestamp_11": [
                        individual_timestamp_11
                    ],
                    "horinzontal_windspeed_height_11": [
                        horinzontal_windspeed_height_11
                    ],
                    "vertical_windspeed_height_11": [
                        vertical_windspeed_height_11
                    ],
                    "wind_direction_11": [
                        wind_direction_11
                    ],
                    "reference": [
                        reference
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
                    "scan_dwell_time": [
                        scan_dwell_time
                    ]
                }
            }
        ]
    }
    payload = json.dumps(dict, indent=4)
    return payload


# Read config.
with open('/home/pi/motion-sensor-box/src/lidar/src/lidar-mqtt.json') as json_file:
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

# Receive data using ZMQ.
ipc_protocol = 'tcp://127.0.0.1'
ipc_port = '5556'
connect_to = f'{ipc_protocol}:{ipc_port}'
print(f'Trying to bind zmq to {connect_to}')

ctx = zmq.Context()
zmq_socket = ctx.socket(zmq.SUB)

try:
    zmq_socket.connect(connect_to)
except Exception as e:
    logging.fatal(f'Failed to bind to zeromq socket: {e}')
    sys.exit(-1)

# Subscribe to all available data.
zmq_socket.setsockopt(zmq.SUBSCRIBE, b'')

print('Successfully bound to zeroMQ receiver socket as subscriber')

is_first_data = True

print('Trying to receive data.')
try:
    while True:
        (zmq_topic, data) = zmq_socket.recv_multipart()
        zmq_topic = zmq_topic.decode('utf-8')
        data = pickle.loads(data)
        if is_first_data:
            print(f'Received first data: {data}')
            print(f'From topic: {zmq_topic}')
            is_first_data = False
        if zmq_topic == "ldr":  # filling the .json structure with the data published by zmqPUB.py
            payload = create_mqtt_payload(set_height_1=data[4],set_height_2=data[10],set_height_3=data[16],
                        set_height_4=data[22],set_height_5=data[28],set_height_6=data[34],set_height_7=data[40],set_height_8=data[46],set_height_9=data[52],set_height_10=data[58],set_height_11=data[64],
                        individual_reference_1=data[5],individual_reference_2=data[11],individual_reference_3=data[17],individual_reference_4=data[23],individual_reference_5=data[29],
                        individual_reference_6=data[35],individual_reference_7=data[41],individual_reference_8=data[47],individual_reference_9=data[53],individual_reference_10=data[59],individual_reference_11=data[65],
                        individual_timestamp_1=str(data[6]),individual_timestamp_2=str(data[12]),individual_timestamp_3=str(data[18]),individual_timestamp_4=str(data[24]),individual_timestamp_5=str(data[30]),individual_timestamp_6=str(data[36]),
                        individual_timestamp_7=str(data[42]),individual_timestamp_8=str(data[48]),individual_timestamp_9=str(data[54]),individual_timestamp_10=str(data[60]),individual_timestamp_11=str(data[66]),
                        horinzontal_windspeed_height_1=data[7],horinzontal_windspeed_height_2=data[13],horinzontal_windspeed_height_3=data[19],horinzontal_windspeed_height_4=data[25],horinzontal_windspeed_height_5=data[31],horinzontal_windspeed_height_6=data[37],
                        horinzontal_windspeed_height_7=data[43],horinzontal_windspeed_height_8=data[49],horinzontal_windspeed_height_9=data[55],horinzontal_windspeed_height_10=data[61],horinzontal_windspeed_height_11=data[67],
                        vertical_windspeed_height_1=data[8],vertical_windspeed_height_2=data[14],vertical_windspeed_height_3=data[20],vertical_windspeed_height_4=data[26],vertical_windspeed_height_5=data[32],vertical_windspeed_height_6=data[38],
                        vertical_windspeed_height_7=data[44],vertical_windspeed_height_8=data[50],vertical_windspeed_height_9=data[56],vertical_windspeed_height_10=data[62],vertical_windspeed_height_11=data[68],
                        wind_direction_1=data[9],wind_direction_2=data[15],wind_direction_3=data[21],wind_direction_4=data[27],wind_direction_5=data[33],wind_direction_6=data[39],wind_direction_7=data[45],
                        wind_direction_8=data[51],wind_direction_9=data[57],wind_direction_10=data[63],wind_direction_11=data[69],
                        temperature=data[70],battery=data[71],airPressure=data[72],ground_windspeed=data[73],tilt=data[74],humidity=data[75],raining=data[76],
                        met_wind_direction=data[77],pod_upper_temperature=data[78],pod_lower_temperature=data[79],pod_humidity=data[80],gps_latitude=data[81],
                        gps_longitude=data[82],timestamp_data_received=str(data[0]),reference=data[2],timestamp_data_generated=str(data[3]),scan_dwell_time=data[83])
            client.publish(mqtt_topic, payload)
        else:
            print(f'Only use topic "imu" or "ldr" is used, however I received data on topic: {zmq_topic}')
        continue
except KeyboardInterrupt:
    client.loop_stop()
    print('Interrupted!')
    pass
# except Exception as e:
#    print(f'Failed to receive message: {e} ({topic} : {data})')
#    continue