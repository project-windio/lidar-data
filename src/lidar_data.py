#import of necessary moduls
import struct
import zmq
from pyModbusTCP.client import ModbusClient
from time import sleep
from datetime import datetime, timezone, timedelta
from time import time
import logging
import sys
import uptime
import pickle
import calendar

class Lidar():
    """
    Using the Modbus capability of the ZX300 it is possible to receive data using the python module "pyModbus".
    The received data is stored in a list and send internally using ZMQ.
    """
    
    def __init__(self):
        """
        Creates all attributes that are required to access ZX300's Modbus registers and to establish a 
        ZMQ connection on Motion Sensor Box.
        """

        self.client = ModbusClient(host="10.10.8.1", port=502, auto_open=True)  # connect Motion Sensor Box with ZX300
        self.zmq_connection = ('tcp://127.0.0.1:5556')
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        
        # Dictionaries for the Modbus register numbers. Some register numbers can be found in ZX300's Modbus Guide
        # ("Modbus_registers_x.png" and README.md) and some were found by reverse engineering.
        self.met_station_dic = {"temperature": 20, "battery": 18, "airPressure": 22, "windspeed": 32, "tilt": 42,
                                "humidity": 24, "raining": 34, "met_wind_direction": 10,
                                "pod_upper_temperature": 26, "pod_lower_temperature": 28, "pod_humidity": 30,
                                "gps_latitude": 44, "gps_longitude": 46, "scan_dwell_time": 8228}
        self.horizontal_windspeed_dic = {"height_1": 2, "height_2": 258, "height_3": 514, "height_4": 770,
                                         "height_5": 1026, "height_6": 1282,
                                         "height_7": 1538, "height_8": 1794, "height_9": 2050, "height_10": 2306,
                                         "height_11": 2562}
        self.vertical_windspeed_dic = {"height_1": 4, "height_2": 260, "height_3": 516, "height_4": 772,
                                         "height_5": 1028, "height_6": 1284,
                                         "height_7": 1540, "height_8": 1796, "height_9": 2052, "height_10": 2308,
                                         "height_11": 2564}
        self.wind_direction_dic = {"height_1": 6, "height_2": 262, "height_3": 518, "height_4": 774,
                                         "height_5": 1030, "height_6": 1286,
                                         "height_7": 1542, "height_8": 1798, "height_9": 2054, "height_10": 2310,
                                         "height_11": 2566}
        self.heights_dic = {"height_1": 8202, "height_2": 8204, "height_3": 8206, "height_4": 8208, "height_5": 8210,
                            "height_6": 8212, "height_7": 8214,
                            "height_8": 8216, "height_9": 8218, "height_10": 8220, "height_11": 8224}
        self.reference_dic = {"height_1": 0, "height_2": 256, "height_3": 512, "height_4": 768, "height_5": 1024,
                              "height_6": 1280, "height_7": 1536,
                              "height_8": 1792, "height_9": 2048, "height_10": 2304, "height_11": 2560}
        self.height_list = ["height_1", "height_2", "height_3", "height_4", "height_5", "height_6", "height_7",
                            "height_8", "height_9", "height_10", "height_11"]

        #since the only way to know whether a new data set is available is to compare the reference numbers, it is
        #necessary to store the values for the first height in variables because the values for the first height
        #are refreshed at the same time as the reference number
        self.vertical_windspeed_1 = None
        self.horizontal_windspeed_1 = None
        self.wind_direction_1 = None

        self.horizontal_windspeed_list = []
        self.vertical_windspeed_list = []
        self.wind_direction_list = []
        self.heights_list = []
        self.reference = None
        self.data = [] * 84
        self.counter = 0

    def build_connection(self):
        """
        Build connection to the ZX300 using Modbus TCP/IP.
        """

        try:
            self.client.open() == True
            print("connection established")
        except Exception as e:
            print(f"unable to connect to lidar:{e}")
            sys.exit(0)

    def bind_zmq_connection(self):
        """
        Bind a ZeroMQ connection for intern communication. The data extracted from the lidar
        is published and gets received by the script lidar_mqtt.py running on the motion sensor box.
        """

        logging.debug(f'binding to {self.zmq_connection} for zeroMQ IPC')
        try:
            self.socket.bind(self.zmq_connection)
            print(f"successfully established zmq connection")
        except Exception as e:
            logging.fatal('failed to connect to zeroMQ socket for IPC')
            sys.exit(-1)
        logging.debug(f'connected to zeroMQ IPC socket')
        logging.debug(f'entering endless loop')

    def publish_data(self):
        """
        Publish all data internally using ZeroMQ.
        """
        LIDAR_TOPIC = "ldr".encode('utf-8')  # setting the Topic for ZMQ
        try:
            lidar_data = self.socket.send_multipart([LIDAR_TOPIC,pickle.dumps(self.data)])
        except Exception as e:
            print(f"unable to send data with zmq: {e}")
            sys.exit(-1)

    def start_up(self):
        """
        Essential method to receive information about the number of heights and
        the set heights before polling data in a loop.
        """
        self.number_of_heights = int(self.get_lidar_data(8200))
        self.reference = self.get_lidar_data(0)
        j = 0
        while j <= self.number_of_heights:
            for key in self.horizontal_windspeed_dic:
                try:
                    self.heights_list.append(self.get_lidar_data(self.heights_dic.get(key)))
                    j += 1
                except:
                    self.heights_list.append(f"set height value is missing")
                    j += 1

    def run(self):
        """
        The run function is the main method for receiving and sending data. Everytime the reference of a dataset changes
        all data in the important registers is received, computed and later sent internally using ZeroMQ. The reference
        changes each time when the lidar has made a full measurement (meaning all heights and the corresponding values
        are measured). The data is temporarily stored in a list (self.data)
        """
        self.first_data = True
        while self.client.open() == True:
            self.referenceCur = self.get_lidar_data(0)
            if self.reference == self.referenceCur:
                if self.first_data == True:
                    self.met_station_data = self.output_met_station()
                    self.vertical_windspeed_1 = self.get_lidar_data(self.vertical_windspeed_dic[self.height_list[0]])
                    self.wind_direction_1 = self.get_lidar_data(self.wind_direction_dic[self.height_list[0]])
                    self.horizontal_windspeed_1 = self.get_lidar_data(self.horizontal_windspeed_dic[self.height_list[0]])
                    self.timestamp = self.get_lidar_time_stamp()
                    self.first_data = False
                else:
                    sleep(0.2)
            else:
                self.first_data = True
                self.data = [str(calendar.timegm(datetime.fromtimestamp(timestamp=time(), tz=timezone.utc).utctimetuple())), uptime.uptime()]
                self.data.append(self.reference)
                self.data.append(self.timestamp)
                self.horizontal_windspeed_list.clear()
                self.counter = 0
                while self.counter < self.number_of_heights:
                    self.data.append(self.heights_list[self.counter])
                    self.poll_reference()
                    self.individual_timestamp()
                    self.polling_horinzontal_windspeeds()
                    self.polling_vertical_windspeeds()
                    self.polling_wind_directions()
                    self.counter += 1
                self.data.extend(self.met_station_data)

                for i in range(len(self.data)):
                    if self.data[i] == 9999.0 or self.data[i] == 9998:
                        self.data[i] = None

                self.publish_data()
                self.data.clear()
                self.reference = self.referenceCur

    def polling_vertical_windspeeds(self):
        """
        Appends the value for the vertical wind speed at the given height to the list.
        """
        try:
            if self.counter == 0:
                self.data.append(self.vertical_windspeed_1)
            else:
                self.data.append(self.get_lidar_data(self.vertical_windspeed_dic[self.height_list[self.counter]]))
        except Exception as e:
            self.data.append(None)
            print(f"unable to receive value for this height(vertical_windspeed):{e}")

    def polling_wind_directions(self):
        """
        Appends the value for the wind direction at the given height to the list.
        """
        try:
            if self.counter == 0:
                self.data.append(self.wind_direction_1)
            else:
                self.data.append(self.get_lidar_data(self.wind_direction_dic[self.height_list[self.counter]]))
        except Exception as e:
            self.data.append(None)
            print(f"unable to receive value for this height(wind direction):{e}")

    def polling_horinzontal_windspeeds(self):
        """
        Appends the value for the horizontal wind speed at the given height to the list.
        """
        try:
            if self.counter == 0:
                self.data.append(self.horizontal_windspeed_1)
            else:
                self.data.append(self.get_lidar_data(self.horizontal_windspeed_dic[self.height_list[self.counter]]))
        except Exception as e:
                self.data.append(None)
                print(f"unable to receive value for this height(horizontal_windspeed):{e}")

    def poll_reference(self):
        """
        Append an individual reference number to the list (self.data) for each measurement made by the ZX300 .
        Each height gets a unique reference number.
        """
        try:
            if self.counter == 0:
                self.current_reference = self.reference
                self.data.append(self.current_reference)
            else:
                self.current_reference = self.get_lidar_data(self.reference_dic[self.height_list[self.counter]])
                self.data.append(self.current_reference)

        except Exception as e:
                self.data.append(None)
                print(f"unable to receive reference number for this height(horizontal_windspeed):{e}")


    def output_met_station(self):
        """
        Poll all important data which is measured by the met station and data concerning the status of the ZX300.

        Return
        ------
        list
            containing all met station data and data concerning the status of the lidar
            Shape: len = 14
        """
        try:
            self.met_station_data = [round(self.get_lidar_data(self.met_station_dic.get("temperature")),2),
                              round(self.get_lidar_data(self.met_station_dic.get("battery")),2),
                              round(self.get_lidar_data(self.met_station_dic.get("airPressure")),2),
                              round(self.get_lidar_data(self.met_station_dic.get("windspeed")),2),  #windspeed measured by the met station (appr. 1,5 meter above the ground)
                              round(self.get_lidar_data(self.met_station_dic.get("tilt")),2),
                              round(self.get_lidar_data(self.met_station_dic.get("humidity")),2),
                              round(self.get_lidar_data(self.met_station_dic.get("raining")),2),
                              round(self.get_lidar_data(self.met_station_dic.get("met_wind_direction")),2),  #0° = North
                              round(self.get_lidar_data(self.met_station_dic.get("pod_upper_temperature")),2),
                              round(self.get_lidar_data(self.met_station_dic.get("pod_lower_temperature")),2),
                              round(self.get_lidar_data(self.met_station_dic.get("pod_humidity")),2),  #humidity within the pod
                              round(self.get_lidar_data(self.met_station_dic.get("gps_latitude")),2),
                              round(self.get_lidar_data(self.met_station_dic.get("gps_longitude")),2),
                              round(self.get_lidar_data(self.met_station_dic.get("scan_dwell_time")),2)]#time it takes to measure one height (usally 1 second)
            return self.met_station_data
        except Exception as e:
            print(f"unable to receive all data provided by the met station:{e}")

    def get_lidar_time_stamp(self):
        """
        Since the timestamp is 32 bit and the mantissa is only the size of 23 bits, the information
        for the timestamp is split into two registers.
        These two registers are combined and processed subsequently.

         Return
         ------
         time object
            timestamp object representing the time the data was measured
            Shape: timestamp with year|month|day|hour|minute|second|timezone info
         """

        self.timestamp_dic = {"TS_top": None ,"TS_bottom":None }
        self.timestampTop = self.client.read_input_registers(36,2)
        self.timestampBottom= self.client.read_input_registers(38,2)
        for i in self.timestampTop:
            if i == None:
                return False
            else:
                self.timestamp_dic["TS_top"] = self.hex_to_float(self.timestampTop[0],self.timestampTop[1])

        for j in self.timestampBottom:
            if j == None:
                return False
            else:
                self.timestamp_dic["TS_bottom"] = self.hex_to_float(self.timestampBottom[0],self.timestampBottom[1])
        timestamp_add = self.timestamp_dic["TS_top"] + self.timestamp_dic["TS_bottom"]
        #print("timestamp_add",timestamp_add)

        year_stamp = timestamp_add / 60 / 60 / 24 / 31 / 12
        year_cal = str(year_stamp).split(".")
        year = "20" + str(year_cal[0])
        # print(year,"year")
        month_cal = self.cal_date(12, year_cal)
        month = round(int(month_cal[0]), 8)
        # print(month,"month")
        day_cal = self.cal_date(31, month_cal)
        day = day_cal[0]
        # print(day,"day")
        hour_cal = self.cal_date(24, day_cal)
        hour = hour_cal[0]
        # print(hour,"hour")
        minute_cal = self.cal_date(60, hour_cal)
        minute = minute_cal[0]
        # print(minute,"minute")
        sec_cal = self.cal_date(60, minute_cal)
        second = sec_cal[0]
        # print(second,"second")
        tz = timezone(timedelta(hours=0))

        try:
            day_in_month = calendar.monthrange(int(year), int(month) - 1)[1]


        except:
            day_in_month = None
            pass
        try:

            self.timestamp = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=tz)
            return self.timestamp

        except ValueError:
            if int(month) == 0:
                year = int(year) - 1
                month = int(month) + 12
                if int(day) == 0:
                    day = 1
                self.timestamp = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second),
                                          tzinfo=tz)
                return self.timestamp

            if int(day) == 0 and int(month) == 0:
                print("IF 2 ")
                day = int(day) + 31
                self.timestamp = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second),
                                          tzinfo=tz)
                return self.timestamp

            if day_in_month == 31 and int(month) != 3:
                month = int(month) - 1
                day = int(day) + 31
                self.timestamp = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second),
                                          tzinfo=tz)
                return self.timestamp

            if day_in_month == 31 and int(month) == 3:
                month = 2
                day = 1
                self.timestamp = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second),
                                          tzinfo=tz)
                return self.timestamp

            if int(month) == 1 and int(day) == 0:
                month = 12
                year = int(year) - 1
                day = 31
                self.timestamp = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second),
                                          tzinfo=tz)
                return self.timestamp



    def cal_date(self,factor, decimal):
        """
        multiply remaining decimal with factor (depending on the calculation) and splitting the result

        Parameters
        ----------
        factor: int
                factor by which the remaining decimal needs to be multiplied with

        decimal: list
                 remaining decimal number of the previous calculation
        Return
        ------
        list
            returning the remaining value as a list
            Shape: len = 2
        """
        stamp = round(float("0." + str(decimal[1])), 8) * factor
        stamp_new = f'{stamp:.10f}'
        cal = str(stamp_new).split(".")
        return cal


    def individual_timestamp(self):
        """
        Since the ZX300 does not measure all horizontal wind speeds at the same time, individual
        timestamps are calculated, based on the scan_dwell_time. The scan_dwell_time is added to the
        timestamp of the first timestamp.
        The ZX300 outputs the highest placed height in the beginning and iterates through the rest
        from highest to lowest.
        """
        timeobject = self.timestamp + timedelta(seconds=self.counter)
        self.data.append(calendar.timegm(timeobject.utctimetuple()))


    def get_lidar_data(self, attribute):
        """
        poll data from the ZX300 based on the register number.
        'hex' is a list which contains two decimal numbers which represent the data contained in the specific register.

        Parameter
        --------
        attribute: int
                   register number
        Return
        ------
        float
            returning the converted value in the specific register number
            Shape : 1-dimensional
        """

        hex = self.client.read_input_registers(attribute,2)
        for i in hex:
            if i == None:
                return False
        return self.hex_to_float(hex[0], hex[1])


    def hex_to_float(self, hex0, hex1):
        """
        dec_to_float converts the registers (decimal_numbers) to hex/decimal numbers.
        The two hex/decimal numbers are than combined. It is important to note, that
        all data is stored in big endian format.
        The combined hex/decimal number is later translated to binary code which is than interpreted as a single
        precision float number (32bits).

        Parameter
        ---------
        hex0: float
              first value contained in the register
        hex1: float
              second contained in the register

        Return
        ------
        float
            calculated value contained in the register
            Shape: 1-dimensional
        """
        hexA_long = hex(hex0)
        hexB_long = hex(hex1)
        try:
            hexA_list = hexA_long.split("x")
            hexB_list = hexB_long.split("x")
            if len(hexA_list[1]) < 4:
                hexA_list[1] = str(0) + hexA_list[1]
                hexCom = hexB_list[1] + hexA_list[1] #combined hex/decimal number
            else:
                hexCom = hexB_list[1] + hexA_list[1]  # combined hex/decimal number
        except:
            hexCom = hexB_long+hexA_long
        while len(str(hexCom)) < 8: #add 0 at the end if necessary
            hexCom += "0"
        binary = int(bin(int(hexCom, 16))[2:].zfill(32),2) #interpret the hex/decimal number as binary
        return round(struct.unpack('f', binary.to_bytes(4, "little"))[0], 4)  # translate to float 32 bit

    def end(self):
        print(f"connection to lidar is terminated, program shutdown ....")

if __name__ == "__main__":
    Fx = Lidar()
    Fx.bind_zmq_connection()
    Fx.build_connection()
    Fx.start_up()
    Fx.run()
    Fx.end()
