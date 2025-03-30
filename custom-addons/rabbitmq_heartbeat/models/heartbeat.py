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

# Only use the variables that are already in docker-compose.yml
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'guest')

# RabbitMQ queue and routing key for the heartbeat
QUEUE_NAME = 'controlroom.heartbeat'
ROUTING_KEY = 'controlroom.heartbeat.ping'

# Module-level variables for singleton implementation
_thread_lock = threading.Lock()
_heartbeat_thread_instance = None
_is_thread_running = False

class HeartbeatThread(threading.Thread):
    """Thread that sends a heartbeat to RabbitMQ at specified intervals."""
    
    def __init__(self):
        super().__init__()
        self.daemon = True  # Ensures thread stops when Odoo stops
        self.running = True
        self.connection = None
        self.channel = None
        self.instance_id = id(self)
        _logger.info(f"HeartbeatThread instance created with ID: {self.instance_id}")

    def run(self):
        """Sends heartbeat to RabbitMQ at regular intervals."""
        global _is_thread_running
        
        try:
            self._setup_connection()
            _logger.info(f"HeartbeatThread starting with ID: {self.instance_id}")
            _is_thread_running = True
            
            while self.running:
                try:
                    current_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    heartbeat_msg = self.create_heartbeat_message()
                    
                    # Publish to the specified queue and routing key
                    self.channel.basic_publish(
                        exchange='',  # Using default exchange
                        routing_key=ROUTING_KEY,
                        body=heartbeat_msg,
                        properties=pika.BasicProperties(delivery_mode=2)  # Persistent messages
                    )
                    
                    _logger.info(f"[HEARTBEAT] Sent heartbeat at {current_time}")
                    time.sleep(0.5)  # Fixed interval of 0.5 seconds
                except pika.exceptions.AMQPConnectionError:
                    _logger.warning("Lost connection to RabbitMQ. Attempting to reconnect...")
                    self._setup_connection()
        except Exception as e:
            _logger.error(f"Heartbeat thread error: {e}")
        finally:
            with _thread_lock:
                _is_thread_running = False
                global _heartbeat_thread_instance
                if _heartbeat_thread_instance == self:
                    _heartbeat_thread_instance = None
            _logger.info(f"HeartbeatThread stopping with ID: {self.instance_id}")
            self._close_connection()
    
    def _setup_connection(self):
        """Establishes connection to RabbitMQ."""
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=credentials,
            connection_attempts=3,
            retry_delay=5
        )
        
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        
        # Declare the queue
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
        """Generates a simple XML heartbeat message."""
        root = ET.Element("Heartbeat")
        
        # Add ServiceName element
        service_name = ET.SubElement(root, "ServiceName")
        service_name.text = "Odoo_POS"
        
        # Add Status element
        status = ET.SubElement(root, "Status")
        status.text = "OK"
        
        # Add Timestamp element
        timestamp = ET.SubElement(root, "Timestamp")
        timestamp.text = datetime.datetime.utcnow().isoformat() + "Z"
        
        # Add Host
        host = ET.SubElement(root, "Host")
        host.text = socket.gethostname()
        
        # Convert to string and return
        return ET.tostring(root, encoding="utf-8", method="xml").decode()


class RabbitMQHeartbeat(models.AbstractModel):
    _name = 'rabbitmq.heartbeat'
    _description = 'RabbitMQ Heartbeat Service'

    @api.model
    def start_heartbeat(self):
        """Start the heartbeat thread if it's not already running."""
        global _thread_lock, _heartbeat_thread_instance, _is_thread_running
        
        with _thread_lock:
            # Check if the thread is already running
            if _is_thread_running:
                _logger.info("Heartbeat thread is already running")
                return False
                
            # Create and start a new thread
            _heartbeat_thread_instance = HeartbeatThread()
            _logger.info("Starting heartbeat service...")
            _heartbeat_thread_instance.start()
            return True

    @api.model
    def stop_heartbeat(self):
        """Stop the heartbeat thread."""
        global _thread_lock, _heartbeat_thread_instance
        
        with _thread_lock:
            if _heartbeat_thread_instance and _heartbeat_thread_instance.is_alive():
                _logger.info(f"Stopping heartbeat service...")
                _heartbeat_thread_instance.stop()
                return True
            return False
        
    @api.model
    def reset_heartbeat(self):
        """Reset the heartbeat state (for testing/debugging)."""
        global _thread_lock, _heartbeat_thread_instance, _is_thread_running
        
        with _thread_lock:
            if _heartbeat_thread_instance:
                if _heartbeat_thread_instance.is_alive():
                    _heartbeat_thread_instance.stop()
                _heartbeat_thread_instance = None
                _is_thread_running = False
                _logger.info("Heartbeat thread has been reset")
                return True
            return False

class RabbitMQHeartbeatStartup(models.AbstractModel):
    _name = "rabbitmq.heartbeat.startup"
    _description = "Start RabbitMQ Heartbeat at Odoo startup"

    @api.model
    def _register_hook(self):
        """Start the heartbeat thread at Odoo startup."""
        # Add a small delay to prevent module reload issues
        self._cr.execute("SELECT pg_sleep(2)")
        self.env['rabbitmq.heartbeat'].start_heartbeat()
