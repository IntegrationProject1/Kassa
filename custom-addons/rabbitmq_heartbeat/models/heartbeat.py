import pika
import threading
import time
import datetime
import socket
import xml.etree.ElementTree as ET
from odoo import models, api
from odoo.service import common

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
        credentials = pika.PlainCredentials('guest', 'guest') # Default credentials
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=credentials
        ))
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
        
    
    def stop(self):
        """Stop de thread netjes."""
        self.running = False
        
        
    def create_heartbeat_message(self):
        """Genereert een XML heartbeat bericht in het gewenste formaat."""
        # Create the root element
        root = ET.Element("Heartbeat")
    
        # Add ServiceName element
        service_name = ET.SubElement(root, "ServiceName")
        service_name.text = "Odoo_POS" 
    
        # Add Status element
        status = ET.SubElement(root, "Status")
        status.text = "OK"
        
        # Add Timestamp element -> ISO format
        timestamp = ET.SubElement(root, "Timestamp")
        timestamp.text = datetime.datetime.utcnow().isoformat() + "Z"  # Adding Z for UTC timezone
        
        # Add HeartBeatInterval element
        interval = ET.SubElement(root, "HeartBeatInterval")
        interval.text = str(HEARTBEAT_INTERVAL)
        
        # Add Metadata section
        metadata = ET.SubElement(root, "Metadata")
        
        # Add Version in Metadata
        version = ET.SubElement(metadata, "Version")
        version.text = "1.0.0"  #whats the actual verion idk
        
        # Add Host in Metadata
        host = ET.SubElement(metadata, "Host")
        host.text = socket.gethostname() 
        
        # Add Environment in Metadata
        environment = ET.SubElement(metadata, "Environment")
        environment.text = "production" 
        
        # Convert to str and return
        return ET.tostring(root, encoding="utf-8", method="xml").decode()    


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

class RabbitMQHeartbeatStartup(models.AbstractModel):
    _name = "rabbitmq.heartbeat.startup"
    _description = "Start RabbitMQ Heartbeat bij Odoo opstart"

    @api.model
    def _register_hook(self):
        """Start de heartbeat-thread bij Odoo startup."""
        self.env['rabbitmq.heartbeat'].start_heartbeat()
