
import redis
import random
import time
import pandas as pd
from datetime import datetime
from pydantic import BaseModel
import matplotlib
from fastapi import FastAPI
from dateutil.parser import isoparse
app = FastAPI()

HOST = '127.0.0.1'
PORT = 6379
class Singleton(type):
    """
    An metaclass for singleton purpose. Every singleton class should inherit from this class by 'metaclass=Singleton'.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class RedisClient(metaclass=Singleton):

    def __init__(self):
        #self.pool = redis.ConnectionPool(host = HOST, port = PORT, password = PASSWORD)
        self.pool = redis.ConnectionPool(host = HOST, port = PORT)

    @property
    def conn(self):
        if not hasattr(self, '_conn'):
            self.getConnection()
        return self._conn

    def getConnection(self):
        self._conn = redis.Redis(connection_pool = self.pool)

class Measurement(BaseModel):
    sensor: str
    timestamp: datetime
    #epoch: int
    val: float

rc = RedisClient()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/power/{x}")
def read_item(x: int):
    return x*x

@app.get("/get/{x}/{y}")
def read_item(x: int, y:int):
    return x+y

@app.post("/store/{sensor}/{timestamp}/{val}")
def store1(sensor:str, timestamp: str, val:float):
    # store this value into db
    # return ack msg
    try:
        rc.conn.ts().add(sensor, timestamp, val)
        return "OK!"
    except Exception as e:
        Print(e)
        return "Error!"



@app.post("/store")
async def store3(measurement: Measurement):
    try:
        #epoch = measurement.epoch
        timestamp = measurement.timestamp
        epoch = int(timestamp.timestamp()*1000)
        sensor = measurement.sensor
        val = measurement.val
        print(f"{datetime.now()} - {sensor} {timestamp} {epoch} {val}")
        rc.conn.ts().add(sensor, epoch, val)
        return "OK!"
    except Exception as e:
        print(e)
        return "Error!"
    
@app.get("/getdata1/{sensor}/{from_time}/{to_time}")
def getdata(sensor:str, from_time: str, to_time:str):
    # get data between two timestamps
    try:
        rescl=redis.Redis(host=HOST,port=PORT)           
        device_name = sensor
        startepoch = from_time
        endepoch=to_time
        res = rescl.ts().range(device_name, startepoch, endepoch)
        df=pd.DataFrame(res)
        df.columns = ["datetime", "value"]
        return df.to_json()
        # return "READ SUCCESSFUL!"
    except Exception as e:
        Print(e)
        return "Error!"
    
@app.get("/getdata/{sensor}/{from_time}/{to_time}")
def getdata(sensor:str, from_time: str, to_time:str):
    # get data between two timestamps
    try:
        rescl=redis.Redis(host=HOST,port=PORT)           
        device_name = sensor
        startiso = from_time
        endiso=to_time
        startepoch = int(isoparse(startiso).timestamp())*1000
        endepoch=int(isoparse(endiso).timestamp())*1000
        res = rescl.ts().range(device_name, startepoch, endepoch)
        df=pd.DataFrame(res)
        df.columns = ["datetime", "value"]
        df.datetime = pd.to_datetime(df.datetime, unit="ms", utc=True).map(lambda x: x.tz_convert('Asia/Kolkata'))
        return df.to_dict()
        # return "READ SUCCESSFUL!"
    except Exception as e:
        if e.args[0]=="TSDB: the key does not exist":
            print("ERROR READ..SENSOR NOT FOUND.!")
            return "ERROR READ..SENSOR NOT FOUND.!"
        else:
            if e.args[0]=="TSDB: wrong fromTimestamp": 
                print("ERROR READ..INCORRECT TIME INPUT.!")
                return "ERROR READ INCORRECT TIME INPUT"
            else:
                if e.args[0].startswith('Length mismatch: Expected axis has 0 elements'):
                    if startepoch > endepoch:
                        print ("ERROR.START TIME GREATER THAN END TIME ")
                        return "ERROR.START TIME GREATER THAN END TIME"
                    else:
                        print("NO VALUES IN THE TIME INTERVAL")
                        return "NO VALUES IN THE TIME INTERVAL"
                else:
                    if e.args[0].startswith('invalid literal for int() with base 10'):
                        print("INCORRECT ISO TIME FORMAT")
                        return "INCORRECT ISO TIME FORMAT"
                    else :
                        return e.args[0]
                    
@app.get("/aggregate/{sensor}/{from_time}/{to_time}/{agr_function}/{time_unit}")
async def aggregate(sensor:str, from_time: str, to_time:str,agr_function:str,time_unit:int):
    # get data between two timestamps
    try:
        rescl=redis.Redis(host=HOST,port=PORT)           
        device_name = sensor
        startiso = from_time
        timebucket= time_unit
        agr_fun = agr_function
        endiso=to_time
        startepoch = int(isoparse(startiso).timestamp())*1000
        endepoch=int(isoparse(endiso).timestamp())*1000
        res = rescl.ts().range(device_name, startepoch, endepoch,None,agr_fun,timebucket)
        df=pd.DataFrame(res)
        df.columns = ["datetime", "value"]
        df.set_index ('datetime')
        return df.to_dict()
        # return "READ SUCCESSFUL!"
    except Exception as e:
        if e.args[0]=="TSDB: the key does not exist":
            print("ERROR READ..SENSOR NOT FOUND.!")
            return "ERROR READ..SENSOR NOT FOUND.!"
        else:
            if e.args[0]=="TSDB: wrong fromTimestamp": 
                print("ERROR READ..INCORRECT TIME INPUT.!")
                return "ERROR READ INCORRECT TIME INPUT"
            else:
                if e.args[0].startswith('Length mismatch: Expected axis has 0 elements'):
                    if startepoch > endepoch:
                        print ("ERROR.START TIME GREATER THAN END TIME ")
                        return "ERROR.START TIME GREATER THAN END TIME"
                    else:
                        print("NO VALUES IN THE TIME INTERVAL")
                        return "NO VALUES IN THE TIME INTERVAL"
                else:
                    if e.args[0].startswith('invalid literal for int() with base 10'):
                        print("INCORRECT ISO TIME FORMAT")
                        return "INCORRECT ISO TIME FORMAT"
                    else :
                        if e.args[0]=="TSDB: Unknown aggregation type" :
                            print("AGGREGATION TYPE INCORRECT")
                            return "AGGREGATION TYPE INCORRECT"
                        else:
                            if e.args[0]=="TSDB: bucketDuration must be greater than zero":
                                print("BUCKET DURATION TO BE GREATER THAN ZERO")
                                return "BUCKET DURATION TO BE GREATER THAN ZERO"
                            else:
                                print(e)
                                return e.args[0]
