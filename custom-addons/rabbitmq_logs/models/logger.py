import logging
import threading
import time
import pika
import os
import xml.etree.ElementTree as ET
from lxml import etree
import datetime
import sys
import queue
from odoo import models, api

# Constants
LOG_PREFIX = "[RABBITMQ_LOGS]"

_logger = logging.getLogger(__name__)

# RabbitMQ configuration
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'integrationproject-2425s2-001.westeurope.cloudapp.azure.com')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 30020)) 
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_EXCHANGE = 'log_monitoring'
RABBITMQ_QUEUE = 'controlroom.log.event'
ROUTING_KEY = 'controlroom.log.event'


# XML Schema
LOG_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="Log">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="ServiceName" type="xs:string"/>
        <xs:element name="Status" type="xs:string"/>
        <xs:element name="Message" type="xs:string"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>'''

# Global variables
log_queue = queue.Queue()
log_thread = None
connected = False

def print_log(message):
    """Print a log message with timestamp"""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print(f"{timestamp} {LOG_PREFIX} {message}", file=sys.stderr)

def create_log_message(service_name, status, code, message):
    """Create XML log message"""
    root = ET.Element("Log")
    
    ET.SubElement(root, "ServiceName").text = service_name
    ET.SubElement(root, "Status").text = status
    ET.SubElement(root, "Code").text = code
    ET.SubElement(root, "Message").text = message
    
    return ET.tostring(root, encoding="utf-8", method="xml").decode()

def send_log_to_queue(service_name, status, code, message):
    """Add a log message to the queue"""
    # Skip logs from the logger module itself to prevent recursion
    if "rabbitmq_logs" in service_name.lower():
        return
    
    # Truncate message if too long
    if message and len(message) > 2000:
        message = message[:1997] + "..."
        
    # Create XML message and add to queue
    xml_message = create_log_message(service_name, status, code, message)
    log_queue.put(xml_message)

def log_sender_thread():
    """Thread that sends logs to RabbitMQ"""
    global connected
    connection = None
    channel = None
    
    while True:
        # Connect to RabbitMQ if not connected
        if not connected:
            try:
                credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
                parameters = pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials,
                    heartbeat=600
                )
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                
                # Declare exchange and queue
                channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type='direct', durable=True)
                channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
                channel.queue_bind(exchange=RABBITMQ_EXCHANGE, queue=RABBITMQ_QUEUE, routing_key=ROUTING_KEY)
                
                connected = True
                print_log("Connected to RabbitMQ logger service")
                
                # Send initialization message
                init_message = create_log_message(
                    "Odoo_POS", 
                    "INFO", 
                    "SYSTEM_INIT", 
                    f"Odoo RabbitMQ logger initialized at {datetime.datetime.now().isoformat()}"
                )
                channel.basic_publish(
                    exchange=RABBITMQ_EXCHANGE,
                    routing_key=ROUTING_KEY,
                    body=init_message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type='application/xml'
                    )
                )
                
            except Exception as e:
                print_log(f"Failed to connect to RabbitMQ: {e}")
                time.sleep(10)  # Wait before trying to reconnect
                continue

        # Process log messages from queue
        try:
            try:
                # Try to get message with timeout
                log_message = log_queue.get(block=True, timeout=5.0)
                
                # Publish message
                channel.basic_publish(
                    exchange=RABBITMQ_EXCHANGE,
                    routing_key=ROUTING_KEY,
                    body=log_message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type='application/xml'
                    )
                )
                log_queue.task_done()
            except queue.Empty:
                # No message in queue, just continue
                pass
                
        except pika.exceptions.AMQPError:
            print_log("RabbitMQ connection lost, reconnecting...")
            connected = False
            
            # Close connection if it exists
            try:
                if connection and connection.is_open:
                    connection.close()
            except:
                pass
                
        except Exception as e:
            print_log(f"Error in log sender thread: {e}")

class RabbitMQLogHandler(logging.Handler):
    """Custom handler to send logs to RabbitMQ - only for ERRORS and system messages"""
    def emit(self, record):
        try:
            # Only process errors and warnings
            if record.levelno < logging.WARNING:
                return
                
            # Format the log message
            message = self.format(record)
        
            # Determine status from log level
            if record.levelno >= logging.ERROR:
                status = "ERROR"
            else:
                status = "WARNING"
            
            # Use the same service name as heartbeat: Odoo_POS
            service_name = "Odoo_POS"
            
            # Generate code from logger name
            code = f"LOG_{record.name.replace('.', '_').upper()}"
            
            # Send log to queue
            send_log_to_queue(service_name, status, code, message)
            
        except Exception as e:
            print(f"Error in RabbitMQ log handler: {e}")

class RabbitMQLogStarter(models.AbstractModel):
    _name = 'rabbitmq.log.starter'
    _description = 'RabbitMQ Log Starter'
    
    @api.model
    def _register_hook(self):
        """Start the logging integration when Odoo starts"""
        global log_thread
        
        # Start log sender thread if not running
        if log_thread is None or not log_thread.is_alive():
            log_thread = threading.Thread(target=log_sender_thread, daemon=True)
            log_thread.start()
        
        # Add log handler to root logger for errors only
        handler = RabbitMQLogHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        handler.setLevel(logging.WARNING)  # Only WARNING and ERROR
        logging.getLogger().addHandler(handler)
        
        return super(RabbitMQLogStarter, self)._register_hook()

# Start the logging system automatically with a delay
def delayed_start():
    time.sleep(3)  # Give Odoo time to start
    
    # Add log handler to root logger
    handler = RabbitMQLogHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    handler.setLevel(logging.WARNING)  # Only WARNING and ERROR
    logging.getLogger().addHandler(handler)
    
    # Start thread
    global log_thread
    if log_thread is None or not log_thread.is_alive():
        log_thread = threading.Thread(target=log_sender_thread, daemon=True)
        log_thread.start()

# Start automatically
threading.Thread(target=delayed_start, daemon=True).start()