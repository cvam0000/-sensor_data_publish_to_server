import csv
import requests
import time
import threading
import os

global unpublish_data
unpublish_data=[]


def connection():
    try:
        requests.get('http://localhost:8080/') #port 8080 is defined in the server program
        return True
    except:
        return False    

def failure_handling(packet):
    unpublish_data.append([packet,False])
    
    if connection():
        t1 = threading.Thread(target=publish_data_to_server, name='t1') #Thread will do normal publishing
        t2 = threading.Thread(target=publish_unpublish_data, name='t2') #Thread will publish unpublish data after every 5 secs
        t1.start()
        t2.start()
  
        # wait until all threads finish
        t1.join()
        t2.join()
    else:
        publish_data_to_server()    

def publish_unpublish_data():

    for tmp in unpublish_data:
        
        if tmp[1]==False:
            r=requests.post('http://localhost:8080/',data=tmp[0])
            print(r.json)
            time.sleep(5) #waiting for 5 sec to publish unpublish data
    return True
        


def read_file():
    with open('dataset.csv') as data_points:
        data_file=csv.reader(data_points, delimiter=',')
        line_count=0
        
        data_dict={'sensor_data':[],'time_stamp':[]}
        for row in data_file:
            data_dict['sensor_data'].append(row[1])
            data_dict['time_stamp'].append(row[0])
        return data_dict    

def publish_data_to_server():
    data=read_file()
    #single_packet={'time':[],'data':[]}
    counter=0
    data_sensor=[]
    for tmp in data:
        data_sensor.append(data[tmp])
    
    del(data_sensor[0][0]) #filtering unwanted data
    del(data_sensor[1][0])

    starting_point=len(unpublish_data)
    for tmp in range(starting_point,len(data_sensor[0])):
        packet={'time':[],'data':[]}
        packet['data'].append( data_sensor[0][ tmp ])
        packet['time'].append( data_sensor[1][ tmp ])
        print(packet)
        
        if connection():
            r=requests.post('http://localhost:8080/',data=packet)
            print(r.json)
            time.sleep(60) #waiting for 60 secs to publish the data.
            unpublish_data.append([packet,True])
            #print(unpublish_data)
            del(packet)

        else:
            
            failure_handling(packet)
            
        


if __name__=='__main__':
    
    publish_data_to_server()
    print(unpublish_data)
