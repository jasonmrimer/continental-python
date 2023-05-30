import sys

from kafka import KafkaConsumer, KafkaProducer
import json


bootstrap_servers = 'localhost:9092'
consumer_topic = sys.argv[1]
producer_topic = sys.argv[2]

consumer = KafkaConsumer(
    consumer_topic,
    bootstrap_servers=bootstrap_servers,
    group_id='python_consumer'
)

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

for message in consumer:
    message_value = json.loads(message.value)
    program_id = message_value['ProgramId']
    loop_counter = message_value['LoopCounter']

    print(
        f"Python received message: Program ID={program_id}, Loop Counter={loop_counter}"
    )

    # Increase the loop counter
    loop_counter += 1

    # Create the response message
    response_message = {'ProgramId': program_id, 'LoopCounter': loop_counter}

    # Serialize the response message to JSON
    response_json = json.dumps(response_message)

    # Produce the response message to the response topic
    producer.send(producer_topic, value=response_json.encode())

    # Stop when the loop counter reaches 100
    if loop_counter >= 6:
        break

consumer.close()
producer.close()

print("python finished")