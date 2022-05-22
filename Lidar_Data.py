#import of necessary moduls
import pickle
import struct
from pyModbusTCP.client import ModbusClient
from time import sleep
from datetime import datetime, timezone, timedelta
from time import time

class Lidar():
    def __init__(self):
        self.client = ModbusClient(host="10.10.8.1", port=502, auto_open=True) #building connection to Lidar-Unit
        #the dictionary contains the Modbus register numbers to the specific keys
        self.Met_station_dic = {"temperature":20,"battery":18, "airPressure": 22, "windspeed": 32, "tilt":42, "humidity":24}
        self.horizontal_windspeed_dic = {"height_1":2 ,"height_2":258, "height_3":514, "height_4":770, "height_5":1026, "height_6":1282,
                                        "height_7":1538, "height_8":1794, "height_9":2050, "height_10":2306,"height_11":2562}
        self.heights_dic = {"height_1":8202,"height_2":8204,"height_3":8206,"height_4":8208,"height_5":8210,"height_6":8212,"height_7":8214,
                            "height_8":8216,"height_9":8218,"height_10":8220,"height_11":8224}
        self.horizontal_windspeed_list = []
        self.heights_list = []
        #check if connection is established or failed
        if self.client.open() == True:
            print("connection established")
        else:
            print("connection failed")
            return None

        for key in self.horizontal_windspeed_dic:
            try:
                self.horizontal_windspeed_list.append(self.get_windspeed(self.horizontal_windspeed_dic.get(key)))
                self.heights_list.append(self.get_windspeed(self.heights_dic.get(key)))

            except:
                self.horizontal_windspeed_list.append("one value is missing")

        reference = self.get_MET_station_data(0)
        while self.client.open() == True: #as long as the connection is established the following methods will be called
            #pulling live-data from Lidar-Unit
            referenceCur = self.get_MET_station_data(0)

            #if a new data set is available, this data is saved in a pickle file.
            if reference == referenceCur :
                sleep(1)
            else:
                self.horizontal_windspeed_list.clear()
                for key in self.horizontal_windspeed_dic:
                    try:
                        self.horizontal_windspeed_list.append(self.get_windspeed(self.horizontal_windspeed_dic.get(key)))
                    except:
                        self.horizontal_windspeed_list.append("one value is missing")
                self.pickle_data()

                reference = referenceCur

    # this method outputs data from the MET-Station
    def output_Met_station(self):
        #this dictionary contains all important data measured by the met_station
        met_station ={"temperature":self.get_MET_station_data(self.Met_station_dic.get("temperature")),
                      "battery":self.get_MET_station_data(self.Met_station_dic.get("battery")),
                      "airPressure":self.get_MET_station_data(self.Met_station_dic.get("airPressure")),
                      "windspeed":self.get_MET_station_data(self.Met_station_dic.get("windspeed")),
                      "tilt":self.get_MET_station_data(self.Met_station_dic.get("tilt")),
                      "humidity":self.get_MET_station_data(self.Met_station_dic.get("humidity"))}

        """
        print("_________________________________________________________________")
        print(self.get_MET_station_data(self.Met_station_dic.get("temperature")),"[°C], temperature")
        print(self.get_MET_station_data(self.Met_station_dic.get("battery")),"[V], battery state")
        print(self.get_MET_station_data(self.Met_station_dic.get("airPressure")),"[mPa], air pressure")
        print(self.get_MET_station_data(self.Met_station_dic.get("windspeed")),"[m/s], wind speed")
        print(self.get_MET_station_data(self.Met_station_dic.get("tilt")),"[°], tilt")
        print(self.get_MET_station_data(self.Met_station_dic.get("humidity")),"[%], humidity")
        print("_________________________________________________________________")
        """

        return met_station


    #this method outputs every horizontal windspeed at set heights refreshing every 10 seconds in the console
    #this method is rather for test purposes when setting up the lidar-unit at the location
    def output_horizontal_windspeed(self):
        while True:
            print("________________________________________")
            for key in self.horizontal_windspeed_dic:
                try:
                    self.horizontal_windspeed_list.append(self.get_windspeed(self.horizontal_windspeed_dic.get(key)))
                    print(self.get_windspeed(self.horizontal_windspeed_dic.get(key)),"[m/s]")
                except:
                    self.horizontal_windspeed_list.append("one value is missing")
                    print("one value is missing")
            print("________________________________________")
            sleep(10)

    def get_MET_station_data(self, attribute):
        hex = self.client.read_input_registers(attribute,2)
        for i in hex:
            if i == None:
                return False
        return self.dec_to_float(hex[0], hex[1])

    def get_windspeed(self,height):
        hex = self.client.read_input_registers(height, 2)
        for i in hex:
            if i == None:
                return False
        return self.dec_to_float(hex[0],hex[1])

    def get_time_stamp(self):
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
        timestamp = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=tz)
        return timestamp


    def cal_date(self,factor, decimal):
        stamp = float("0." + str(decimal[1])) * factor
        cal = str(stamp).split(".")
        return cal


    #for further implementation of the data on other systems, it is possible to generate a pickle file. The pickle file
    #contains a dictionary of the horizontal windspeeds, the Met-station data, a time stamp generated when data is received, a time stamp when data is generated by the lidar and
    #a reference number for the individual data sets
    def pickle_data(self):
        horizontal_windspeed = {}
        set_heights = {}
        height_list = ["height_1","height_2","height_3","height_4","height_5","height_6","height_7","height_8","height_9","height_10","height_11"]
        for i in range (11):
            horizontal_windspeed[height_list[i]] = self.horizontal_windspeed_list[i]
            set_heights[height_list[i]] = self.heights_list[i]

        dic ={"met_station":self.output_Met_station(),"horizontal_windspeed": horizontal_windspeed,"set_heights": set_heights, "time_stamp_data_received": time(),"time_stamp_data_generated": self.get_time_stamp(), "reference": self.get_MET_station_data(0)}
        with open("Windspeed.pickle", "wb") as pickle_file:
            pickle.dump(dic,pickle_file)
        #the pickle file is later send to a server on which the data is used for a digital twin

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

ZX300 = Lidar()