



# !pip install git+https://github.com/dpkp/kafka-python.git
import six
import sys

if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves
# from kafka import KafkaConsumer

# # Create a Kafka consumer
# consumer = KafkaConsumer('foobar', bootstrap_servers=['localhost:9092'])

# # Consume messages from the topic
# for msg in consumer:
#     print(msg)

from kafka import KafkaProducer
import time
import json
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON format
)
 
# File path to the taxi trip data
data_file = r"yellow_tripdata_2015-01.csv"  # Replace with your data file
 
# Stream data line by line
with open(data_file, 'r') as file:
    # Skip the header line
    header = next(file)
    
    for line in file:
        # Split the line into fields and create a dictionary
        fields = line.strip().split(',')
        data = {
            'VendorID': fields[0],
            'pickup_datetime': fields[1],
            'dropoff_datetime': fields[2],
            'passenger_count': fields[3],
            'trip_distance': fields[4],
            'pickup_longitude': fields[5],
            'pickup_latitude': fields[6],
            'RateCodeID': fields[7],
            'store_and_fwd_flag': fields[8],
            'dropoff_longitude': fields[9],
            'dropoff_latitude': fields[10],
            'payment_type': fields[11],
            'fare_amount': fields[12],
            'extra': fields[13],
            'mta_tax': fields[14],
            'tip_amount': fields[15],
            'tolls_amount': fields[16],
            'improvement_surcharge': fields[17],
            'total_amount': fields[18],
        }
        
        # Send data to Kafka topic
        producer.send('foobar', value=data)
        print(f"Sent: {data}")
    
        # Simulate streaming by adding a delay
        time.sleep(1)  # Adjust the delay as needed
 
# Close the producer
producer.close() 