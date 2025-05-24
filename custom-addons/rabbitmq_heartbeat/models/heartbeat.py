import pika
import threading
import time
import datetime
import socket
import logging
import xml.etree.ElementTree as ET
from lxml import etree
import os
import io
from odoo import models, api, fields

_logger = logging.getLogger(__name__)

# RabbitMQ configuration - try environment variables with defaults
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT'))
EXCHANGE_NAME = 'heartbeat_monitoring'  # Updated exchange name
QUEUE_NAME = 'controlroom.heartbeat.ping'
ROUTING_KEY = 'controlroom.heartbeat.ping'  # Updated routing key
HEARTBEAT_INTERVAL = 1

# Heartbeat XSD Schema
HEARTBEAT_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="Heartbeat">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="ServiceName" type="xs:string"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>'''

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
        self.xsd_schema = None
        try:
            xsd_doc = etree.XML(HEARTBEAT_XSD.encode('utf-8'))
            self.xsd_schema = etree.XMLSchema(xsd_doc)
            log_message("XSD schema for heartbeat validation loaded successfully")
        except Exception as e:
            log_message(f"Error loading XSD schema: {str(e)}")
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
                channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct', durable=True)  # Changed to direct
                channel.queue_declare(queue=QUEUE_NAME, durable=True)
                channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)  # Added routing key
                log_message("Exchange and queue setup complete")
                
                # Send heartbeat messages in a loop
                while self.running:
                    current_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    heartbeat_msg = self.create_heartbeat_message()
                    
                    # Validate the XML against XSD schema before sending
                    if self.validate_xml(heartbeat_msg):
                        log_message(f"Publishing heartbeat at {current_time}")
                        channel.basic_publish(
                            exchange=EXCHANGE_NAME,
                            routing_key=ROUTING_KEY,  # Using specific routing key
                            body=heartbeat_msg,
                            properties=pika.BasicProperties(
                                delivery_mode=2,  # Persistent messages
                                content_type='application/xml'
                            )
                        )
                        log_message(f"Heartbeat sent at {current_time}")
                    else:
                        log_message(f"Heartbeat message validation failed, message not sent")
                        
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
        """Generates a simplified XML heartbeat message according to the new XSD schema."""
        root = ET.Element("Heartbeat")
        
        # Add ServiceName element - only field needed per new schema
        service_name = ET.SubElement(root, "ServiceName")
        service_name.text = "Kassa"
        
        # Convert to string and return
        xml_message = ET.tostring(root, encoding="utf-8", method="xml").decode()
        return xml_message
    
    def validate_xml(self, xml_string):
        """Validate XML string against the XSD schema."""
        if not hasattr(self, 'xsd_schema') or self.xsd_schema is None:
            try:
                # Recreate the schema on first use
                xsd_no_declaration = HEARTBEAT_XSD.replace('<?xml version="1.0" encoding="UTF-8"?>', '')
                xsd_doc = etree.XML(xsd_no_declaration.encode('utf-8'))
                self.xsd_schema = etree.XMLSchema(xsd_doc)
                log_message("XSD schema for heartbeat validation loaded on first use")
            except Exception as e:
                log_message(f"Failed to load XSD schema: {e}")
                return True  # Continue without validation rather than failing
        
        try:
            # Remove XML declaration from message before validation
            xml_content = xml_string.replace('<?xml version="1.0" encoding="UTF-8"?>', '')
            if xml_content.startswith('\n'):
                xml_content = xml_content.strip()
            
            xml_doc = etree.XML(xml_content.encode('utf-8'))
            is_valid = self.xsd_schema.validate(xml_doc)
            
            if not is_valid:
                for error in self.xsd_schema.error_log:
                    log_message(f"XML validation error: {error}")
                return False
                
            return True
        except Exception as e:
            log_message(f"Error during XML validation: {type(e).__name__} - {str(e)}")
            return False


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
