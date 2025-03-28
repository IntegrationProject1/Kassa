import pika
import json

RABBITMQ_HOST = 'localhost'  
QUEUE_NAME = 'crm_user_create'  # name of the queue to send the message to

# message
message = {
    "name": "Test1",
    "email": "test.@example.com"
}

# connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
channel = connection.channel()

# make sure the queue exists
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# Send message to the queue
channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=json.dumps(message))

print(f"testmessage send to {QUEUE_NAME}: {message}")
connection.close()
