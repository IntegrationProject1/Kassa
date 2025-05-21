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
RABBITMQ_HOST     = os.environ['RABBITMQ_HOST']
RABBITMQ_PORT     = int(os.environ['RABBITMQ_PORT'])
RABBITMQ_USER     = os.environ['RABBITMQ_USER']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
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

def create_log_message(service_name, status, message):
    """Create XML log message"""
    root = ET.Element("Log")
    
    ET.SubElement(root, "ServiceName").text = service_name
    ET.SubElement(root, "Status").text = status
    ET.SubElement(root, "Message").text = message
    
    return ET.tostring(root, encoding="utf-8", method="xml").decode()

def send_log_to_queue(service_name, status, message):
    """Add a log message to the queue"""
    # Skip logs from the logger module itself to prevent recursion
    if "rabbitmq_logs" in service_name.lower():
        return
    
    # Truncate message if too long
    if message and len(message) > 2000:
        message = message[:1997] + "..."
        
    # Create XML message and add to queue - ignore the code parameter
    xml_message = create_log_message(service_name, status, message)
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
    """Custom handler to send logs to RabbitMQ - now captures INFO logs too"""
    def emit(self, record):
        try:
            # Filter out certain loggers that generate too much noise
            if record.name in ['werkzeug', 'odoo.http']:
                return
            
            # Skip heartbeat-related logs to avoid duplication
            if "HEARTBEAT" in record.name.upper() or \
               (hasattr(record, 'msg') and isinstance(record.msg, str) and 
                ("heartbeat" in record.msg.lower() or 
                 "[HEARTBEAT_MODULE]" in record.msg)):
                return
                
            # Format the log message
            message = self.format(record)
        
            # Determine status from log level
            if record.levelno >= logging.ERROR:
                status = "ERROR"
            elif record.levelno >= logging.WARNING:
                status = "WARNING"
            else:
                status = "INFO"
            
            # Use the same service name as heartbeat: Odoo_POS
            service_name = "Odoo_POS"
            
            # Look for module identifiers like [ORDER_MODULE], [CUSTOMER_CREATE_MODULE], etc.
            if hasattr(record, 'msg') and isinstance(record.msg, str):
                message_str = record.msg
                module_tags = [
                    "[ORDER_MODULE]", 
                    "[CUSTOMER_CREATE_MODULE]",
                    "[CUSTOMER_UPDATE_MODULE]",
                    "[CUSTOMER_DELETE_MODULE]",
                    "[USER_DELETE_MODULE]"
                ]
                
                for tag in module_tags:
                    if tag in message_str:
                        # Override the code with a more specific one based on the module tag
                        module_name = tag.strip('[]')
                        break
            
            # Send log to queue
            send_log_to_queue(service_name, status, message)
            
        except Exception as e:
            print(f"Error in RabbitMQ log handler: {e}")

class RabbitMQLogStarter(models.AbstractModel):
    _name = 'rabbitmq.log.starter'
    _description = 'RabbitMQ Log Starter'
    
    @api.model
    def _register_hook(self):
        """Start the logging integration when Odoo starts"""
        global log_thread
        
        # Start send‐thread if needed
        if log_thread is None or not log_thread.is_alive():
            log_thread = threading.Thread(target=log_sender_thread, daemon=True)
            log_thread.start()
        
        # Create & configure handler
        handler = RabbitMQLogHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        
        # *** Ensure the root logger will emit INFO ***
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(handler)
        
        return super(RabbitMQLogStarter, self)._register_hook()

# Start the logging system automatically with a delay
def delayed_start():
    time.sleep(3)  # Give Odoo time to start

    handler = RabbitMQLogHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)

    # ensure INFO-level messages from other addons propagate
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)           # <-- add this
    root_logger.addHandler(handler)

    # Start thread
    global log_thread
    if log_thread is None or not log_thread.is_alive():
        log_thread = threading.Thread(target=log_sender_thread, daemon=True)
        log_thread.start()

# Start automatically
threading.Thread(target=delayed_start, daemon=True).start()

def log_customer_event(action, customer_name, customer_id, external_id=None):
    """Log customer creation/update/deletion events"""
    details = f"{customer_name} (ID: {customer_id}"
    if external_id:
        details += f", External ID: {external_id}"
    details += ")"
    
    send_log_to_queue(
        "Odoo_POS",
        "INFO",
        f"CUSTOMER_{action.upper()}",
        f"Customer {action}: {details}"
    )

def log_order_event(action, order_id, partner_name=None, product_count=None):
    """Log POS order events"""
    details = f"Order {order_id}"
    if partner_name:
        details += f" for {partner_name}"
    if product_count:
        details += f" with {product_count} product(s)"
    
    send_log_to_queue(
        "Odoo_POS",
        "INFO",
        f"ORDER_{action.upper()}",
        f"Order {action}: {details}"
    )

def log_event_event(action, event_name, event_id, uuid=None):
    """Log event creation/update/deletion"""
    details = f"{event_name} (ID: {event_id}"
    if uuid:
        details += f", UUID: {uuid}"
    details += ")"
    
    send_log_to_queue(
        "Odoo_POS",
        "INFO",
        f"EVENT_{action.upper()}",
        f"Event {action}: {details}"
    )

def log_billing_event(event_name, event_id, user_count):
    """Log event billing operations"""
    send_log_to_queue(
        "Odoo_POS",
        "INFO",
        "EVENT_BILLING",
        f"Event billing completed for {event_name} (ID: {event_id}): {user_count} users processed"
    )