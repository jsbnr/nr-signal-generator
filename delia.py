import time
from datetime import datetime, timedelta, timezone
import json
import schedule
import os
import subprocess
import sys
import random
import requests
import threading
import pandas as pd
import scipy
from configuration import get_configuration as conf

package_uuid = os.getenv('NEWRELIC_PACKAGE_UUID') 
document_id = os.getenv('NEWRELIC_DOCUMENT_ID') 
account_id = os.getenv('NEWRELIC_ACCOUNT_ID')
newrelic_api_key = os.getenv('NEWRELIC_API_KEY')
newrelic_user_key = os.getenv('NEWRELIC_USER_KEY') 

config_check_frequency=60
if('CONFIG_CHECK_FREQUENCY' in os.environ):
     config_check_frequency = int(os.getenv('CONFIG_CHECK_FREQUENCY'))

current_configurations = {}
env_vars_checked = False

def grab_conf():
    if not env_vars_checked:
        check_env_vars()
    config = conf.get_config(newrelic_user_key,package_uuid,account_id,document_id)
    print("Configuration received from Synthetic Configurator: " + str(json.dumps(config)))
    return config

def check_env_vars():
    global env_vars_checked
    keys = ("NEWRELIC_PACKAGE_UUID","NEWRELIC_DOCUMENT_ID","NEWRELIC_ACCOUNT_ID","NEWRELIC_API_KEY","NEWRELIC_USER_KEY")
    keys_not_set = []

    for key in keys:
        if key not in os.environ:
            keys_not_set.append(key)
    else:
        pass

    if len(keys_not_set) > 0: 
        for key in keys_not_set:
            print(key + " not set")
        exit(1)
    else:
        env_vars_checked = True
        print("All environment variables set, starting signal generator.")

def config_check():
    global current_configurations
    configurations = grab_conf() 
    if str(configurations) !=  str(current_configurations):
        print("Configuration changed, reloading...")
        subprocess.call([sys.executable, os.path.realpath(__file__)] + sys.argv[1:])
    else:
        print("Configuration did not change, continuing with existing configuration...")

class signal:

    def send_to_nr(self,metrics):  
        url = self.config['metrics_url']
        headers = {'Content-Type': 'application/json', 'Api-Key': newrelic_api_key}
        response = requests.post(url, headers=headers, json=metrics)
        now = datetime.now(timezone.utc)
        current_time = now.strftime("%Y/%m/%d %H:%M:%S")
        print(str(current_time)+ " - "+self.config["metric_name"]+": "+ str(len(metrics[0]["metrics"])) + " metrics sent [" + str(response.status_code)+"]" )
        return response.json()

    def sendLatestData(self):
        current_time_unix = int(time.mktime(datetime.today().timetuple()))
        if((current_time_unix-self.this_loop_start_time_unix) > self.duration):
            print("Rebuilding data model for "+self.config["metric_name"])
            self.buildDataModel()
        window_metrics=self.getDataUntilNow()

        def constructMetricEntry(metric):
            return { 
            "name":self.config['metric_name'],
            "type":"gauge",
            "value":metric['value'],
            "timestamp":metric['timestamp'],
            "attributes": {
                "stage" : metric['stage']
                }
            }
        nrFormattedMetrics=list(map(constructMetricEntry,window_metrics))
        metrics = [{
            "metrics": nrFormattedMetrics,
            "common": {
                "attributes": {
                    "source":"signal-generator"
                }
            }
        }]
        self.send_to_nr(metrics=metrics)

    def getDataUntilNow(self):
        current_time_unix = int(time.mktime(datetime.today().timetuple())) - self.dispatch_latency
        seconds_since_start = current_time_unix - self.start_time_unix
        offset = seconds_since_start % self.duration
        
        #first time round then need to set cursor based on window.
        if(self.model_cursor == None):
            self.model_cursor = 0 #self.duration - window
        
        cursor=self.model_cursor

        def metricFilter(obj):
            # This filter returns all the data points for the current required window. 
            # We maintain a cursor of the last data we sent, so each window includes all the data from the last time we reported data.
            # If we're early in the window we might need to include data from the end of the window as the signal loops around.
            include = False
            if(offset > cursor):
                if((int(obj.get('timestamp_offset')) >= cursor) and (int(obj.get('timestamp_offset')) < offset)):
                    include = True
            if(cursor > offset):
                if((int(obj.get('timestamp_offset')) >= cursor) and (int(obj.get('timestamp_offset')) < self.duration)):
                    include = True
                if((int(obj.get('timestamp_offset')) < offset)):
                    include = True
            return include

        window_data = list(filter(metricFilter, self.data_model ))

        self.model_cursor = offset

        #re-map timesstamps based on current time
        def metricTimeAdjust(obj):
            metric_offset =int(obj.get('timestamp_offset'))
            if(metric_offset < offset):
                offset_difference = offset - metric_offset
                new_timestamp = current_time_unix - offset_difference
                obj.update({'timestamp': new_timestamp})
            else:
                offset_difference = offset + (self.duration - metric_offset)
                new_timestamp = current_time_unix - offset_difference
                obj.update({'timestamp': new_timestamp})
            return obj

        re_timed_window_data = list(map(metricTimeAdjust,window_data))
        return re_timed_window_data

    def generateSignalDataFromSparse(self, stage_name, start_offset, data):
        lastMetricTimestamp=0
        metric_data=[]
        offset=0
        size = 0
        for metric in data:
            offset =  start_offset + metric['timestamp']
            size= metric['timestamp'] - lastMetricTimestamp
            metric_payload={
                "value": metric['value'],
                "size": metric['timestamp'] - lastMetricTimestamp,
                "timestamp": self.start_time_unix + metric['timestamp'],
                "timestamp_offset": offset,
                "stage": stage_name
            }
            lastMetricTimestamp=metric['timestamp']
            metric_data.append(metric_payload)
        return metric_data, offset-start_offset, offset+size

    def interpolateSignal(self,signal_data,density,method,reverse):
        def convertToNowTimes(metric):
            now = datetime.now(timezone.utc)
            return {
                "timestamp": now + timedelta(seconds=metric['timestamp']),
                "value": metric['value']
            }

        #convert the model into a signal based upon "now"
        signal_data_now = list(map(convertToNowTimes ,signal_data))
        
        #generate dataframe with data
        df = pd.DataFrame(signal_data_now)
        datetime_index = pd.DatetimeIndex(df['timestamp'].values)
        df=df.set_index(datetime_index) #set the index
        df.drop('timestamp',axis=1,inplace=True) #remove the timestamp field
    
        if (density!=0):
            print("Resampling data at "+str(density)+" seconds using "+method+" interpolation" )
            resample_dataFrame = df.resample(rule=str(density)+'s').mean()
            interpolated_dataFrame = resample_dataFrame.interpolate(method=method, limit_direction='forward', axis=0)
        else: 
            print("Not resampling of data, data will be used raw")
            interpolated_dataFrame=df

        #convert data back to be timestamp seconds since 0
        returnData=[]
        firstTimestamp = 0
        #print("index is" ,str(interpolated_dataFrame.index[0].timetuple()))
        if(reverse == True):
            print("reversing",reverse)
            interpolated_dataFrame[:] = interpolated_dataFrame[::-1] #reverse the value column only!
        for index, row in interpolated_dataFrame.iterrows():
            if( firstTimestamp == 0 ):
                firstTimestamp=index
            returnData.append(
                {
                    "timestamp": int(time.mktime(index.timetuple()))- int(time.mktime(firstTimestamp.timetuple())),
                    "value": row['value']
                }
            )
        return returnData
    


    def generateSignalDataChunk(self, number_metrics, seconds, start_offset, stage_name, low, high, los):
        duration = number_metrics * seconds
        current_metric=0
        offset=start_offset
        metric_data=[]
        while  (current_metric * seconds ) < duration:  
                
            nullData = False
            if( float(los) > 0 ):
                rnd = random.uniform(0, 100) 
                if rnd <= los:
                    nullData = True

            offset = start_offset + (current_metric*seconds)
            if nullData == False: #if null data then simulate loss of data
                metric_payload = { 
                "value":self.generateRandomMetricValue(low,high),
                "size": seconds,
                "timestamp": self.start_time_unix + (current_metric*seconds),
                "timestamp_offset": offset,
                "stage": stage_name
                }
                metric_data.append(metric_payload)
            current_metric+=1
        return metric_data, duration, offset+seconds

    def generateRandomMetricValue(self,low,high):
        if (low != high):
            return round(random.uniform(low, high),2)
        else:
            return low

    def buildDataModel(self):
        self.duration = 0
        self.start_time = datetime.now(timezone.utc)
        if(self.start_time_unix == 0):
            self.start_time_unix = int(time.mktime(self.start_time.today().timetuple()))
        self.this_loop_start_time_unix = int(time.mktime(self.start_time.today().timetuple()))
        self.data_model=[]
        self.dispatch_latency = self.config['dispatch_latency']

        print(str(self.start_time_unix))
        print("Signal initialised at: "+str(self.start_time) + " unix: "+str(self.start_time_unix))
       
        stage_config=self.config['stage_config']

        start_offset=0

        for stage in stage_config:

            if ("sparse_name" in stage):
                # use provided data
                
                dataChunk = self.generateSignalDataFromSparse(stage['sparse_name'], start_offset, self.interpolateSignal(json.loads(stage['sparse_data']),stage['sparse_resample'],stage['sparse_resample_method'],bool(stage['sparse_reverse'])))
                self.data_model+= dataChunk[0]
                self.duration += dataChunk[1]
                start_offset = dataChunk[2]
                print("Signal: "+str(self.config["metric_name"])+" Stage  (sparse data): "+ stage['sparse_name'] + " Duration: "+ str(dataChunk[1]) + " offset:"+str(start_offset))   
            else: 
                # construct data from config
                metrics_per_second = float(stage['frequency'])/60 # convert metrics per minute to per second
                number_metrics = int(int(stage['duration']) * metrics_per_second)
                seconds = int(int(stage['duration']) / number_metrics)

                print("Signal: "+str(self.config["metric_name"])+" Stage: "+ stage['name'] + " Duration: "+str(stage['duration']) + " frequency:"+ str(stage['frequency']) + "pm number_metrics:" + str(number_metrics) + " seconds:" + str(seconds))
                dataChunk = self.generateSignalDataChunk(number_metrics,seconds,start_offset, stage['name'],stage['low'],stage['high'], stage['los'])

                self.data_model+= dataChunk[0]
                self.duration += dataChunk[1]
                start_offset = dataChunk[2]
        
        print("Signal: "+str(self.config["metric_name"])+ " Total duration: "+str(self.duration) + " Dispatch frequency: " + str(self.config['dispatch_frequency'])  + "s Latency: "+str(self.config['dispatch_latency'])+"s") 
        #print(json.dumps(self.data_model))

    def __init__(self,config):
        self.model_cursor=None
        self.start_time_unix=0
        self.config=config
        self.buildDataModel()
        





if __name__ == "__main__":

    schedule.every(config_check_frequency).seconds.do(config_check)


    configurations = grab_conf() 
    current_configurations = configurations
    def run_threaded(job_func):
        job_thread = threading.Thread(target=job_func)
        job_thread.start()

    for i in range(len(configurations)):
        config = configurations[i]

        print("Initialising signal for "+str(config["metric_name"]))
        sig = signal(config=config)
        # print("Model: "+json.dumps(sig.data_model))
        schedule.every(int(config['dispatch_frequency'])).seconds.do(run_threaded,sig.sendLatestData)

    while True:
        schedule.run_pending()
        time.sleep(0.1)