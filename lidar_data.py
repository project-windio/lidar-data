#import of necessary moduls
import pickle
import struct
from pyModbusTCP.client import ModbusClient
from time import sleep
from datetime import datetime, timezone, timedelta
from time import time
import zmqPUB
import lidar_mqtt
from threading import Thread

class Lidar():
    def __init__(self):
        try:
            self.client = ModbusClient(host="10.10.8.1", port=502, auto_open=True) #building connection to Lidar-Unit
            #the dictionary contains the Modbus register numbers to the specific keys
            self.met_station_dic = {"temperature":20, "battery":18, "airPressure": 22, "windspeed": 32, "tilt":42, "humidity":24, "raining":34, "met_wind_direction": 10,
                                    "pod_upper_temperature":26, "pod_lower_temperature": 28, "pod_humidity": 30, "gps_latitude":44, "gps_longitude":46,"scan_dwell_time": 8228}
            self.horizontal_windspeed_dic = {"height_1":2 ,"height_2":258, "height_3":514, "height_4":770, "height_5":1026, "height_6":1282,
                                            "height_7":1538, "height_8":1794, "height_9":2050, "height_10":2306,"height_11":2562}
            self.heights_dic = {"height_1":8202,"height_2":8204,"height_3":8206,"height_4":8208,"height_5":8210,"height_6":8212,"height_7":8214,
                                "height_8":8216,"height_9":8218,"height_10":8220,"height_11":8224}
            self.number_of_heights = int(self.get_lidar_data(8200))
            self.horizontal_windspeed_list = []
            self.heights_list = []
            self.height_list = ["height_1", "height_2", "height_3", "height_4", "height_5", "height_6", "height_7",
                                "height_8", "height_9", "height_10", "height_11"]
            #check if connection is established or failed
            if self.client.open() == True:
                print("connection established")
            else:
                print("connection failed")
                return None
            j = 0
            while j <= self.number_of_heights:
                for key in self.horizontal_windspeed_dic:
                    try:
                        self.horizontal_windspeed_list.append(self.get_lidar_data(self.horizontal_windspeed_dic.get(key)))
                        self.heights_list.append(self.get_lidar_data(self.heights_dic.get(key)))
                        j +=1

                    except:
                        self.horizontal_windspeed_list.append("one value is missing")
                        j += 1

            reference = self.get_lidar_data(0)
            #as long as the connection is established the following methods will be called
            while self.client.open() == True:
                #pulling live-data from Lidar-Unit
                referenceCur = self.get_lidar_data(0)

                #if a new data set is available, this data is saved in a pickle file.
                if reference == referenceCur:
                    sleep(0.2)
                else:
                    self.timestamp_data_received = time()
                    self.horizontal_windspeed_list.clear()
                    j = 0
                    while j <= self.number_of_heights:
                        for key in self.horizontal_windspeed_dic:
                            try:
                                self.horizontal_windspeed_list.append(self.get_lidar_data(self.horizontal_windspeed_dic[key]))
                                j += 1
                            except:
                                self.horizontal_windspeed_list.append("one value is missing")
                                j += 1
                    self.pickle_data()
                    reference = referenceCur
        except:
            sleep(0.2)
            ZX300= Lidar()

    # this method outputs data from the MET-Station
    def output_met_station(self):
        #this dictionary contains all important data measured by the met_station
        self.met_station ={"temperature":self.get_lidar_data(self.met_station_dic.get("temperature")),
                      "battery":self.get_lidar_data(self.met_station_dic.get("battery")),
                      "airPressure":self.get_lidar_data(self.met_station_dic.get("airPressure")),
                      "windspeed":self.get_lidar_data(self.met_station_dic.get("windspeed")),#windspeed measured by the met station (appr. 1,5 meter above the ground)
                      "tilt":self.get_lidar_data(self.met_station_dic.get("tilt")),
                      "humidity":self.get_lidar_data(self.met_station_dic.get("humidity")),
                      "raining":self.get_lidar_data(self.met_station_dic.get("raining")),
                      "met_wind_direction":self.get_lidar_data(self.met_station_dic.get("met_wind_direction")), #0° = North
                      "pod_upper_temperature": self.get_lidar_data(self.met_station_dic.get("pod_upper_temperature")),
                      "pod_lower_temperature": self.get_lidar_data(self.met_station_dic.get("pod_lower_temperature")),
                      "pod_humidity": self.get_lidar_data(self.met_station_dic.get("pod_humidity")),#humidity within the pod
                      "gps_latitude": self.get_lidar_data(self.met_station_dic.get("gps_latitude")),
                      "gps_longitude": self.get_lidar_data(self.met_station_dic.get("gps_longitude")),
                      "scan_dwell_time": self.get_lidar_data(self.met_station_dic.get("scan_dwell_time"))}#time it takes to measure one height (usally 1 second)

        #this print is used for test purposes only
        """ 
        print("_________________________________________________________________")
        print(self.met_station)
        print("_________________________________________________________________")
        """

        return self.met_station


    #this method outputs the horizontal windspeed of height1
    #this method is used for test purposes only when setting up the lidar-unit at the location
    def output_horizontal_windspeed(self):
        while True:
            print("________________________________________")
            j = 0
            while j <= self.number_of_heights:
                for key in self.horizontal_windspeed_dic:
                    try:
                        self.horizontal_windspeed_list.append(self.get_lidar_data(self.horizontal_windspeed_dic.get(key)))
                        print(self.get_lidar_data(self.horizontal_windspeed_dic.get(key)), "[m/s]")
                        j += 1
                    except:
                        self.horizontal_windspeed_list.append("one value is missing")
                        print("one value is missing")
                        j += 1
                print("________________________________________")
                sleep(1)

    def get_lidar_data(self, attribute):
        #'hex' is a list which contains two decimal numbers which represent the data contained in the specific register
        #self.dec_to_float() converts the data into interpretable form
        hex = self.client.read_input_registers(attribute,2)
        for i in hex:
            if i == None:
                return False
        return self.dec_to_float(hex[0], hex[1])

    def get_time_stamp(self):
        #since the timestamp is 32 bit and the mantissa is only the size of 23 bits, the information for the timestamp is split into two registers.
        #these two registers are combined and processed subsequently
        self.timestamp_dic = {"TS_top": None ,"TS_bottom":None }
        self.timestampTop = self.client.read_input_registers(36,2)
        self.timestampBottom= self.client.read_input_registers(38,2)
        for i in self.timestampTop:
            if i == None:
                return False
            else:
                self.timestamp_dic["TS_top"] = self.dec_to_float(self.timestampTop[0],self.timestampTop[1])

        for j in self.timestampBottom:
            if j == None:
                return False
            else:
                self.timestamp_dic["TS_bottom"] = self.dec_to_float(self.timestampBottom[0],self.timestampBottom[1])
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
            timestamp = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=tz)
            return timestamp
        except ValueError:
            month = int(month) -1
            day = int(day) +31
            timestamp = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=tz)
            return timestamp


    def cal_date(self,factor, decimal):
        stamp = float("0." + str(decimal[1])) * factor
        cal = str(stamp).split(".")
        return cal

    #since the lidar does not measure all horizontal windspeeds at the same time, individual timestamps are calculated, based on the scan_dwell_time
    #the lidar outputs the highest placed height in the beginning and iterates trough the rest from heighest to lowest
    def individual_timestamp(self):
        self.individual_timestamp_dic={}
        for i in range (self.number_of_heights):
            self.individual_timestamp_dic[self.height_list[i]] = self.timestamp_data_received - (i + self.met_station["scan_dwell_time"])
        return self.individual_timestamp_dic


    #for further implementation of the data on other systems and projects, it is possible to generate a pickle file. The pickle file
    #contains a dictionary of the horizontal windspeeds, the Met-station data, a time stamp generated when data is received, a time stamp when data is generated by the lidar,
    #the set heights and a reference number for the individual data sets
    def pickle_data(self):
        horizontal_windspeed = {}
        set_heights = {}
        #print(self.number_of_heights)
        for i in range (self.number_of_heights):
            horizontal_windspeed[self.height_list[i]] = self.horizontal_windspeed_list[i]
            set_heights[self.height_list[i]] = self.heights_list[i]

        dic ={"met_station":self.output_met_station(),"horizontal_windspeed": horizontal_windspeed,"set_heights": set_heights,
              "timestamp_data_received": self.timestamp_data_received,"timestamp_data_generated": self.get_time_stamp(),"individual_timestamp": self.individual_timestamp(), "reference": self.get_lidar_data(0)}
        with open("Windspeed.pickle", "wb") as pickle_file:
            pickle.dump(dic,pickle_file)
        #the pickle file is later sent to a server on which the data is used for a digital twin
        #for control purposes it is possible to reopen the pickle file and output the data in the console to
        #control whether the program is runnig properly
        with open("Windspeed.pickle", "rb") as pickle_file:
            output = pickle.load(pickle_file)
        print(output)


    #the method dec_to_float converts the registers (decimal_numbers) to hex/decimal numbers
    #the two hex/decimal numbers are than combined. It is important to note, that
    #all data is stored in big endian format.
    #The combined hex/decimal number is later translated to binary code which is than interpreted as a single
    #precision float number (32bits)
    def dec_to_float(self,hex0,hex1):
        hexA_long = hex(hex0)
        hexB_long = hex(hex1)
        try:
            hexA_list = hexA_long.split("x")
            hexB_list = hexB_long.split("x")
            hexCom = hexB_list[1] + hexA_list[1] #combined hex/decimal number
        except:
            hexCom = hexB_long+hexA_long
        while len(str(hexCom)) < 8: #add 0 at the end if necessary
            hexCom += "0"
        binary = int(bin(int(hexCom, 16))[2:].zfill(32),2) #interpret the hex/decimal number as binary
        return round(struct.unpack('f', struct.pack('i', binary))[0],4) #translate to float 32 bit

if __name__ == "__main__":
    thread_1 = Thread(target = Lidar)
    thread_2 = Thread(target= zmqPUB.main)
    thread_3 = Thread(target=lidar_mqtt.mqtt_connection)
    thread_1.start()
    thread_2.start()
    thread_3.start()






