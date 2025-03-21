import pika
import threading
import time
import datetime
import xml.etree.ElementTree as ET
from odoo import models, api

RABBITMQ_HOST = "rabbitmq"
QUEUE_NAME = "heartbeat"
HEARTBEAT_INTERVAL = 1  # Seconden

class HeartbeatThread(threading.Thread):
    """Thread die elke seconde een heartbeat naar RabbitMQ stuurt."""
    def __init__(self):
        super().__init__()
        self.daemon = True  # Zorgt ervoor dat de thread stopt als Odoo stopt
        self.running = True

    def run(self):
        """Verstuurt elke seconde een heartbeat naar RabbitMQ."""
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        while self.running:
            heartbeat_msg = self.create_heartbeat_message()
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=heartbeat_msg,
                properties=pika.BasicProperties(delivery_mode=2)  # Persistent messages
            )
            print(f"[HEARTBEAT] {heartbeat_msg}")  # Debugging
            time.sleep(HEARTBEAT_INTERVAL)

        connection.close()

    def create_heartbeat_message(self):
        """Genereert een XML heartbeat bericht."""
        root = ET.Element("heartbeat")
        timestamp = ET.SubElement(root, "timestamp")
        timestamp.text = datetime.datetime.utcnow().isoformat()

        status = ET.SubElement(root, "status")
        status.text = "running"

        return ET.tostring(root, encoding="utf-8", method="xml").decode()

    def stop(self):
        """Stop de thread netjes."""
        self.running = False

heartbeat_thread = HeartbeatThread()

class RabbitMQHeartbeat(models.AbstractModel):
    _name = 'rabbitmq.heartbeat'
    _description = 'RabbitMQ Heartbeat Service'

    @api.model
    def start_heartbeat(self):
        """Start de heartbeat-thread als deze nog niet loopt."""
        global heartbeat_thread
        if not heartbeat_thread.is_alive():
            print("Heartbeat service wordt gestart...")
            heartbeat_thread = HeartbeatThread()
            heartbeat_thread.start()



from odoo import api, models
from odoo.service import common

class RabbitMQHeartbeatStartup(models.AbstractModel):
    _name = "rabbitmq.heartbeat.startup"
    _description = "Start RabbitMQ Heartbeat bij Odoo opstart"

    @api.model
    def _register_hook(self):
        """Start de heartbeat-thread bij Odoo startup."""
        self.env['rabbitmq.heartbeat'].start_heartbeat()