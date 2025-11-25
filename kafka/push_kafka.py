from confluent_kafka import Producer
import json
import uuid
import time 
import random as rd
import threading
import os
from hdfs import InsecureClient
import sys

HDFS_PATH = "http://localhost:9870"


producer = Producer({
    'bootstrap.servers' : "localhost:9092"     
})

cilent = InsecureClient(HDFS_PATH , user = "hdfs" )

def call_back(err , msg) :
    if err is not None :
        print(f"Delivery error : {err}")
    else :
        print(f"Delivery to {msg.topic()} [{msg.partition()}] offset={msg.offset()} ")

def sent_batch(batch , topic) :
    producer.produce(topic , key = str(uuid.uuid4()) , value = json.dumps(batch) , callback = call_back)
    print(f"Sent {len(batch)} record to {topic}")

def push_kafka(path , topic , max_record) :
    with cilent.read(path , encoding = 'utf-8') as f :
        data = []
        for line in f :
            line = line.strip()
            data.append(json.loads(line)) 
            if len(data) >= max_record :
                sent_batch(data , topic)
                data = []
                time.sleep(rd.randint(1,3))
        if data :
            sent_batch(data , topic)
        producer.flush()




if __name__ == "__main__" :
    try :
        threads = []
        topics = {
        "/IMDB/data/movie/movies.json" : 'movie' ,
        "/IMDB/data/actor/actors.json" : 'actor' ,
        "/IMDB/data/review/reviews.json" : 'review' 
        }

        for path , topic in topics.items() :
            if topic == 'movie' :
                t = threading.Thread(target = push_kafka , args = (path , topic , 10))
                threads.append(t)
                t.start()
            if topic == 'actor' :
                t = threading.Thread(target = push_kafka , args = (path , topic , 10))
                threads.append(t)
                t.start()
            if topic == 'review' :
                t = threading.Thread(target = push_kafka , args = (path , topic , 100))
                threads.append(t)
                t.start()
        for t in threads : 
            t.join()

        producer.flush()
        
        print("All data sent Sucessfully")
        sys.exit(0)
        
    except Exception as e :
        print("Sent data err : " , str(e) )
        sys.exit(1)