import pika
import threading
import time
import datetime
import socket
import logging
import xml.etree.ElementTree as ET
import os
from odoo import models, api, fields

_logger = logging.getLogger(__name__)

# RabbitMQ configuration - try environment variables with defaults
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT'))
EXCHANGE_NAME = 'heartbeat'
QUEUE_NAME = 'controlroom_heartbeat'
HEARTBEAT_INTERVAL = 1

# Global heartbeat thread
heartbeat_thread = None

def log_message(message):
    print(f"[HEARTBEAT_MODULE] {message}")
    _logger.info(message)

# Log configuration at module load time
log_message(f"Module loaded with config: HOST={RABBITMQ_HOST}, EXCHANGE={EXCHANGE_NAME}, QUEUE={QUEUE_NAME}")

class HeartbeatThread(threading.Thread):
    """Thread that sends a heartbeat to RabbitMQ at specified intervals."""
    
    def __init__(self):
        super().__init__()
        self.daemon = True  # Ensures thread stops when Odoo stops
        self.running = True
        log_message("HeartbeatThread instance created")

    def run(self):
        """Sends heartbeat to RabbitMQ at regular intervals."""
        log_message("HeartbeatThread starting")
        connection = None
        
        while self.running:
            try:
                # Log connection attempt
                log_message(f"Attempting to connect to RabbitMQ at {RABBITMQ_HOST}")
                
                # Create connection with credentials
                credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
                log_message(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=RABBITMQ_HOST,
                        port=RABBITMQ_PORT,
                        credentials=credentials,
                    )
                )
                channel = connection.channel()

                log_message("Connected to RabbitMQ successfully")
                
                # Setup exchange and queue
                log_message(f"Creating exchange: {EXCHANGE_NAME} and queue: {QUEUE_NAME}")
                channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout', durable=True)
                channel.queue_declare(queue=QUEUE_NAME, durable=True)
                channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME)
                log_message("Exchange and queue setup complete")
                
                # Send heartbeat messages in a loop
                while self.running:
                    current_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    heartbeat_msg = self.create_heartbeat_message()
                    
                    log_message(f"Publishing heartbeat at {current_time}")
                    channel.basic_publish(
                        exchange=EXCHANGE_NAME,
                        routing_key='heartbeat',
                        body=heartbeat_msg,
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # Persistent messages
                            content_type='application/xml'
                        )
                    )
                    
                    log_message(f"Heartbeat sent at {current_time}")
                    time.sleep(HEARTBEAT_INTERVAL)
                    
            except pika.exceptions.AMQPConnectionError as e:
                log_message(f"RabbitMQ connection error: {e}")
                log_message(f"Will retry in 5 seconds...")
                time.sleep(5)
            except Exception as e:
                log_message(f"Heartbeat error: {type(e).__name__} - {str(e)}")
                log_message(f"Will retry in 5 seconds...")
                time.sleep(5)  # Wait before trying again
            finally:
                try:
                    if connection and connection.is_open:
                        log_message("Closing connection")
                        connection.close()
                except Exception as e:
                    log_message(f"Error closing connection: {e}")
        
        log_message("HeartbeatThread stopping")
        
    def stop(self):
        """Stops the thread gracefully."""
        log_message("Stop requested for heartbeat thread")
        self.running = False
        
    def create_heartbeat_message(self):
        """Generates a simple XML heartbeat message."""
        root = ET.Element("Heartbeat")
        
        # Add ServiceName element
        service_name = ET.SubElement(root, "ServiceName")
        service_name.text = "Odoo_POS"  # Changed from Odoo_POS to Monitoring
        
        # Add Status element
        status = ET.SubElement(root, "Status")
        status.text = "OK"
        
        # Add Timestamp element
        timestamp = ET.SubElement(root, "Timestamp")
        timestamp.text = datetime.datetime.utcnow().isoformat() + "Z"
        
        # Add HeartBeatInterval element
        heartbeat_interval = ET.SubElement(root, "HeartBeatInterval")
        heartbeat_interval.text = "{HEARTBEAT_INTERVAL}"
        
        # Add Metadata element with nested elements
        metadata = ET.SubElement(root, "Metadata")
        
        # Add Version element under Metadata
        version = ET.SubElement(metadata, "Version")
        version.text = "1.0.0"
        
        # Add Host element under Metadata
        host = ET.SubElement(metadata, "Host")
        host.text = "Azure_VM"
        
        # Add Environment element under Metadata
        environment = ET.SubElement(metadata, "Environment")
        environment.text = "production"
        
        # Convert to string and return
        xml_message = ET.tostring(root, encoding="utf-8", method="xml").decode()
        return xml_message


class RabbitMQHeartbeat(models.AbstractModel):
    _name = 'rabbitmq.heartbeat'
    _description = 'RabbitMQ Heartbeat Service'

    @api.model
    def start_heartbeat(self):
        """Start the heartbeat thread if it's not already running."""
        global heartbeat_thread
        
        log_message("Request to start heartbeat service")
        
        if not heartbeat_thread or not heartbeat_thread.is_alive():
            log_message("Starting new heartbeat thread")
            heartbeat_thread = HeartbeatThread()
            heartbeat_thread.start()
            return True
        else:
            log_message("Heartbeat thread is already running")
            return False

    @api.model
    def stop_heartbeat(self):
        """Stop the heartbeat thread."""
        global heartbeat_thread
        
        log_message("Request to stop heartbeat service")
        
        if heartbeat_thread and heartbeat_thread.is_alive():
            log_message("Stopping active heartbeat thread")
            heartbeat_thread.stop()
            return True
        log_message("No active heartbeat thread to stop")
        return False


class RabbitMQHeartbeatStartup(models.AbstractModel):
    _name = "rabbitmq.heartbeat.startup"
    _description = "Start RabbitMQ Heartbeat at Odoo startup"

    @api.model
    def _register_hook(self):
        """Start the heartbeat thread at Odoo startup."""
        log_message("Initializing heartbeat service at Odoo startup")
        # Add a small delay to prevent module reload issues
        self._cr.execute("SELECT pg_sleep(2)")
        log_message("Starting heartbeat service after delay")
        self.env['rabbitmq.heartbeat'].start_heartbeat()
