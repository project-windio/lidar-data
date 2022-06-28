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
    def __init__(self):
        """

        Using the Modbus capability of the ZX300 Lidar it is possible to extract data by polling data from the
        registers in the dictionaries below. Every register below contains data which refreshes after every full
        measurement. (the time depends on the set number of heights)
        The __init__ function also creates all necessary lists and other variables for either the use of the following
        methods or the Modbus connection between the ZX300 and the MotionSensor Box.

        """

        self.client = ModbusClient(host="10.10.8.1", port=502, auto_open=True)  # building connection to Lidar-Unit
        self.zmq_connection = ('tcp://127.0.0.1:5556')
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        # the dictionary contains the Modbus register numbers to the specific keys

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

    def building_connection(self):
        """
        Building connection to the ZX300 using Modbus TCP/IP.
        """

        try:
            self.client.open() == True
            print("connection established")
        except Exception as e:
            print(f"unable to connect to lidar:{e}")
            sys.exit(0)

    def initiate_zmq_connection(self):
        """
        Initiating a zmq connection for intern communication. The data extracted from the lidar
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
        logging.debug(f'entering endless loop')

    def publishing_data(self):
        """
        All data is published using ZeroMQ.
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
        all data in the important registers is received, computed and later sent using ZeroMQ. The reference changes
        each time when the lidar has made a full measurement (meaning all heights and the corresponding values
        are measured).
        The data is temporarily stored in a list (self.data)
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
                    #print(self.timestamp)
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
                    self.polling_reference()
                    self.individual_timestamp()
                    self.polling_horinzontal_windspeeds()
                    self.polling_vertical_windspeeds()
                    self.polling_wind_directions()
                    self.counter += 1
                self.data.extend(self.met_station_data)

                for i in range(len(self.data)):
                    if self.data[i] == 9999.0 or self.data[i] == 9998:
                        self.data[i] = None

                self.publishing_data()
                self.data.clear()
                self.reference = self.referenceCur

    def polling_vertical_windspeeds(self):
        """
        Appends the value for the vertical wind speeds at the given height to the list.
        """
        try:
            if self.counter == 0:
                self.data.append(self.vertical_windspeed_1)
            else:
                self.data.append(self.get_lidar_data(self.vertical_windspeed_dic[self.height_list[self.counter]]))
        except Exception as e:
            self.data.append("unable to receive value for this height")
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
            self.data.append("unable to receive value for this height")
            print(f"unable to receive value for this height(wind direction):{e}")

    def polling_horinzontal_windspeeds(self):
        """
        Appends the value for the horizontal wind speeds at the given height to the list.
        """
        try:
            if self.counter == 0:
                self.data.append(self.horizontal_windspeed_1)
            else:
                self.data.append(self.get_lidar_data(self.horizontal_windspeed_dic[self.height_list[self.counter]]))
        except Exception as e:
                self.data.append("unable to receive value for this height")
                print(f"unable to receive value for this height(horizontal_windspeed):{e}")

    def polling_reference(self):
        """
        Appending an individual reference number to the list (self.data) for each measurement made by the lidar .
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
                self.data.append("unable to receive reference number for this height")
                print(f"unable to receive reference number for this height(horizontal_windspeed):{e}")


    def output_met_station(self):
        """
        All important data measured by the met station and data concerning the status of the lidar is polled.

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
        These two registers are combined and processed subsequently

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

        year_stamp = timestamp_add / 60 / 60 / 24 / 31 / 12
        year_cal = str(year_stamp).split(".")
        year = "20" + str(year_cal[0])

        month_cal = self.cal_date(12, year_cal)
        month = month_cal[0]

        day_cal = self.cal_date(31, month_cal)
        day = day_cal[0]

        hour_cal = self.cal_date(24, day_cal)
        hour = hour_cal[0]

        minute_cal = self.cal_date(60, hour_cal)
        minute = minute_cal[0]

        sec_cal = self.cal_date(60, minute_cal)
        second = sec_cal[0]

        tz = timezone(timedelta(hours=0))
        #since the lidar does not output a conventional Unix timestamp the calculation returns, on the 31 of the month, a Value Error.
        #To counter this problem the exception decreases the month by one and the day gets increased by 31.
        try:
            self.timestamp = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=tz)
            return self.timestamp
        except ValueError:
            month = int(month) -1
            day = int(day) +31
            self.timestamp = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=tz)
            return self.timestamp

    def cal_date(self,factor, decimal):
        """
        Small calculation for date calculation.

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
        stamp = float("0." + str(decimal[1])) * factor
        cal = str(stamp).split(".")
        return cal


    def individual_timestamp(self):
        """
        Since the lidar does not measure all horizontal wind speeds at the same time, individual
        timestamps are calculated, based on the scan_dwell_time. The scan_dwell_time is added to the
        timestamp of the first timestamp.
        The lidar outputs the highest placed height in the beginning and iterates through the rest
        from highest to lowest.
        """
        timeobject = self.timestamp + timedelta(seconds=self.counter)
        self.data.append(calendar.timegm(timeobject.utctimetuple()))


    def get_lidar_data(self, attribute):
        """
        get_lidar_data is used to poll data from the lidar unit based on the register number.
        'hex' is a list which contains two decimal numbers which represent the data contained in the specific register.
        self.dec_to_float() converts the data into interpretable form.

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
        The method dec_to_float converts the registers (decimal_numbers) to hex/decimal numbers.
        The two hex/decimal numbers are than combined. It is important to note, that
        all data is stored in big endian format.
        The combined hex/decimal number is later translated to binary code which is than interpreted as a single
        precision float number (32bits).

        Parameter
        ---------
        hex0: float
              value contained in the first register
        hex1: float
              value contained in the second register

        Return
        ------
        float
            calculated value contained in the two neighbouring registers
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
    Fx.initiate_zmq_connection()
    Fx.building_connection()
    Fx.start_up()
    Fx.run()
    Fx.end()
