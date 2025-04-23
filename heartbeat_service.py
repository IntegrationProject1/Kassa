# heartbeat_service.py

import pika
import time
import datetime
import xml.etree.ElementTree as ET
import os
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("heartbeat")

# Load environment variables
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
EXCHANGE_NAME = 'heartbeat'
QUEUE_NAME = 'controlroom.heartbeat.ping'
HEARTBEAT_INTERVAL = 1 # send a heartbeat every second

# Validate environment variables
def create_heartbeat_message():
    root = ET.Element("Heartbeat")
    ET.SubElement(root, "ServiceName").text = "Odoo_POS"
    ET.SubElement(root, "Status").text = "OK"
    ET.SubElement(root, "Timestamp").text = datetime.datetime.utcnow().isoformat() + "Z"
    ET.SubElement(root, "HeartBeatInterval").text = str(HEARTBEAT_INTERVAL)
    metadata = ET.SubElement(root, "Metadata")
    ET.SubElement(metadata, "Version").text = "1.0.0"
    ET.SubElement(metadata, "Host").text = "Azure_VM"
    ET.SubElement(metadata, "Environment").text = "production"
    return ET.tostring(root, encoding="utf-8", method="xml").decode()

# Validate RabbitMQ connection parameters
def run_heartbeat():
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
            )
            channel = connection.channel()
            channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout', durable=True)
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME)
            
            while True:
                msg = create_heartbeat_message()
                channel.basic_publish(
                    exchange=EXCHANGE_NAME,
                    routing_key='heartbeat',
                    body=msg,
                    properties=pika.BasicProperties(delivery_mode=2, content_type='application/xml')
                )
                log.info("Sent heartbeat")
                time.sleep(HEARTBEAT_INTERVAL)

        except Exception as e:
            log.error(f"Heartbeat error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run_heartbeat()
