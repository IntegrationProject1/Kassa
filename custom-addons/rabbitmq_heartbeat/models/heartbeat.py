import pika
import threading
import time
import datetime
import socket
import logging
import xml.etree.ElementTree as ET
import os
from odoo import models, api, fields
from odoo.service import common

_logger = logging.getLogger(__name__)

# Configuration with environment variable fallbacks
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
QUEUE_NAME = os.environ.get('RABBITMQ_QUEUE', 'heartbeat')
HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_INTERVAL', '1'))
SERVICE_NAME = os.environ.get('SERVICE_NAME', 'Odoo_POS')
ENVIRONMENT = os.environ.get('ODOO_ENVIRONMENT', 'production')

class HeartbeatThread(threading.Thread):
    """Thread that sends a heartbeat to RabbitMQ at specified intervals."""
    def __init__(self, service_name=None, interval=None):
        super().__init__()
        self.daemon = True  # Ensures thread stops when Odoo stops
        self.running = True
        self.service_name = service_name or SERVICE_NAME
        self.interval = interval or HEARTBEAT_INTERVAL
        self.connection = None
        self.channel = None

    def run(self):
        """Sends heartbeat to RabbitMQ at regular intervals."""
        try:
            self._setup_connection()
            
            while self.running:
                try:
                    heartbeat_msg = self.create_heartbeat_message()
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=QUEUE_NAME,
                        body=heartbeat_msg,
                        properties=pika.BasicProperties(delivery_mode=2)  # Persistent messages
                    )
                    _logger.debug(f"[HEARTBEAT] Sent heartbeat for {self.service_name}")
                    time.sleep(self.interval)
                except pika.exceptions.AMQPConnectionError:
                    _logger.warning("Lost connection to RabbitMQ. Attempting to reconnect...")
                    self._setup_connection()
        except Exception as e:
            _logger.error(f"Heartbeat thread error: {e}")
        finally:
            self._close_connection()
    
    def _setup_connection(self):
        """Establishes connection to RabbitMQ."""
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=credentials,
            connection_attempts=3,
            retry_delay=5
        )
        
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=QUEUE_NAME, durable=True)
        _logger.info(f"Connected to RabbitMQ at {RABBITMQ_HOST}")
    
    def _close_connection(self):
        """Safely closes the RabbitMQ connection."""
        if self.connection and self.connection.is_open:
            self.connection.close()
            _logger.info("RabbitMQ connection closed")
    
    def stop(self):
        """Stops the thread gracefully."""
        self.running = False
        
    def create_heartbeat_message(self):
        """Generates an XML heartbeat message in the required format."""
        # Create the root element
        root = ET.Element("Heartbeat")
    
        # Add ServiceName element
        service_name = ET.SubElement(root, "ServiceName")
        service_name.text = self.service_name
    
        # Add Status element
        status = ET.SubElement(root, "Status")
        status.text = "OK"
        
        # Add Timestamp element -> ISO format
        timestamp = ET.SubElement(root, "Timestamp")
        timestamp.text = datetime.datetime.utcnow().isoformat() + "Z"  # Adding Z for UTC timezone
        
        # Add HeartBeatInterval element
        interval = ET.SubElement(root, "HeartBeatInterval")
        interval.text = str(self.interval)
        
        # Add Metadata section
        metadata = ET.SubElement(root, "Metadata")
        
        # Add Version in Metadata
        version = ET.SubElement(metadata, "Version")
        version.text = "1.0.0"
        
        # Add Host in Metadata
        host = ET.SubElement(metadata, "Host")
        host.text = socket.gethostname() 
        
        # Add Environment in Metadata
        environment = ET.SubElement(metadata, "Environment")
        environment.text = ENVIRONMENT
        
        # Convert to str and return
        return ET.tostring(root, encoding="utf-8", method="xml").decode()    


# Global thread instance
heartbeat_thread = None

class RabbitMQHeartbeat(models.AbstractModel):
    _name = 'rabbitmq.heartbeat'
    _description = 'RabbitMQ Heartbeat Service'
    
    @api.model
    def get_config_param(self, param_name, default=None):
        """Get a configuration parameter from ir.config_parameter."""
        return self.env['ir.config_parameter'].sudo().get_param(f'rabbitmq_heartbeat.{param_name}', default)

    @api.model
    def start_heartbeat(self, service_name=None, interval=None):
        """Start the heartbeat thread if it's not already running."""
        global heartbeat_thread
        
        # Use provided params or fall back to environment variables
        service_name = service_name or SERVICE_NAME
        interval = interval or HEARTBEAT_INTERVAL
        
        if not heartbeat_thread or not heartbeat_thread.is_alive():
            _logger.info(f"Starting heartbeat service for {service_name} every {interval} seconds...")
            heartbeat_thread = HeartbeatThread(service_name=service_name, interval=interval)
            heartbeat_thread.start()
            return True
        return False

    @api.model
    def stop_heartbeat(self):
        """Stop the heartbeat thread."""
        global heartbeat_thread
        if heartbeat_thread and heartbeat_thread.is_alive():
            _logger.info("Stopping heartbeat service...")
            heartbeat_thread.stop()
            return True
        return False

class RabbitMQHeartbeatStartup(models.AbstractModel):
    _name = "rabbitmq.heartbeat.startup"
    _description = "Start RabbitMQ Heartbeat at Odoo startup"

    @api.model
    def _register_hook(self):
        """Start the heartbeat thread at Odoo startup."""
        self.env['rabbitmq.heartbeat'].start_heartbeat()
