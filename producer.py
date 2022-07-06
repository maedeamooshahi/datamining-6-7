from kafka import KafkaProducer
import time
import json
from csv import reader

def myTest(List, counter):
    return {
        'Date/Time': List[counter][0],
        'lat': List[counter][1],
        'lon': List[counter][2],
        'Base': List[counter][3]
    }

def jsonConv(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=jsonConv)

if __name__ == '__main__':
    fileReader = open('data.csv', 'r')
    csvData = reader(fileReader)
    csvList = list(csvData)
    for i in range(1, len(csvList)):
        data = myTest(csvList, i)
        producer.send('testOne', data)
        time.sleep(5)

