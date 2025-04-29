import logging
import threading
import time
import pika
import os
import xml.etree.ElementTree as ET
from lxml import etree
import datetime
import traceback
import io
from odoo import models, api

_logger = logging.getLogger(__name__)

# RabbitMQ configuration
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
EXCHANGE_NAME = 'log_monitoring'
QUEUE_NAME = 'controlroom.log.events'
ROUTING_KEY = 'controlroom.log.events'

# Log XSD Schema
LOG_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="Log">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="ServiceName" type="xs:string"/>
        <xs:element name="Status" type="xs:string"/>
        <xs:element name="Code" type="xs:string"/>
        <xs:element name="Message" type="xs:string"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>'''

# Avoid infinite recursion in logging
def print_only(message):
    print(f"[RABBITMQ_LOGS] {message}")

# Global log queue and thread
log_queue = []
queue_lock = threading.Lock()
log_thread = None
running = False
log_schema = None

try:
    # Setup XML schema for validation
    xsd_root = etree.parse(io.StringIO(LOG_XSD))
    log_schema = etree.XMLSchema(xsd_root)
    print_only("Loaded XML schema for log validation")
except Exception as e:
    print_only(f"Error loading XML schema: {e}")

def create_log_message(service_name, status, code, message):
    """Create XML log message"""
    root = ET.Element("Log")
    
    service_name_elem = ET.SubElement(root, "ServiceName")
    service_name_elem.text = service_name
    
    status_elem = ET.SubElement(root, "Status")
    status_elem.text = status
    
    code_elem = ET.SubElement(root, "Code")
    code_elem.text = code
    
    message_elem = ET.SubElement(root, "Message")
    message_elem.text = message
    
    # Convert to string and return
    xml_message = ET.tostring(root, encoding="utf-8", method="xml").decode()
    return xml_message

def validate_xml(xml_string):
    """Validate XML against schema"""
    if log_schema is None:
        return True  # Skip validation if schema wasn't loaded
        
    try:
        xml_content = xml_string.replace('<?xml version="1.0" encoding="UTF-8"?>', '').strip()
        xml_doc = etree.fromstring(xml_content.encode('utf-8'))
        return log_schema.validate(xml_doc)
    except Exception as e:
        print_only(f"XML validation error: {e}")
        return False

def send_log_to_queue(service_name, status, code, message):
    """Add a log message to the queue"""
    # Don't log messages from this module to avoid recursion
    if "rabbitmq_logs" in service_name.lower():
        return
        
    with queue_lock:
        # Truncate message if too long
        if message and len(message) > 2000:
            message = message[:1997] + "..."
            
        log_queue.append({
            "service_name": service_name,
            "status": status,
            "code": code,
            "message": message,
            "timestamp": datetime.datetime.now().isoformat()
        })

def log_sender_thread():
    """Thread to send logs to RabbitMQ"""
    global running
    running = True
    print_only("Log sender thread started")
    
    while running:
        try:
            # Get messages from queue
            messages_to_send = []
            with queue_lock:
                if log_queue:
                    messages_to_send = log_queue.copy()
                    log_queue.clear()
            
            # If there are messages, send them
            if messages_to_send:
                # Connect to RabbitMQ
                credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=RABBITMQ_HOST,
                        port=RABBITMQ_PORT,
                        credentials=credentials,
                        heartbeat=60  # Higher heartbeat for stability
                    )
                )
                channel = connection.channel()
                
                # Setup exchange and queue
                channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct', durable=True)
                channel.queue_declare(queue=QUEUE_NAME, durable=True)
                channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)
                
                # Send each message
                for log_data in messages_to_send:
                    xml_message = create_log_message(
                        log_data["service_name"], 
                        log_data["status"], 
                        log_data["code"], 
                        log_data["message"]
                    )
                    
                    if validate_xml(xml_message):
                        channel.basic_publish(
                            exchange=EXCHANGE_NAME,
                            routing_key=ROUTING_KEY,
                            body=xml_message,
                            properties=pika.BasicProperties(
                                delivery_mode=2,  # Make message persistent
                                content_type='application/xml'
                            )
                        )
                        print_only(f"Log sent: {log_data['service_name']} - {log_data['code']}")
                    else:
                        print_only(f"Log validation failed: {log_data['message'][:50]}...")
                
                # Close connection
                connection.close()
            
            # Sleep to avoid high CPU usage
            time.sleep(1)
            
        except Exception as e:
            print_only(f"Error in log sender thread: {str(e)}")
            print_only(traceback.format_exc())
            time.sleep(5)  # Wait before retry
    
    print_only("Log sender thread stopped")

# Custom logging handler to capture Odoo logs
class RabbitMQLogHandler(logging.Handler):
    def emit(self, record):
        try:
            # Format the log message
            message = self.format(record)
            
            # Determine status from log level
            if record.levelno >= logging.ERROR:
                status = "ERROR"
            elif record.levelno >= logging.WARNING:
                status = "WARNING"
            else:
                status = "INFO"
            
            # Use module name as service name, fallback to logger name
            if hasattr(record, 'module'):
                service_name = f"Odoo_{record.module.upper()}"
            else:
                service_name = f"Odoo_{record.name.split('.')[-1].upper()}"
            
            # Generate code from logger name
            code = f"LOG_{record.name.replace('.', '_').upper()}"
            
            # Send log to queue (except logs from this module to prevent recursion)
            if 'rabbitmq_logs' not in record.name:
                send_log_to_queue(service_name, status, code, message)
        except Exception as e:
            # Use print to avoid infinite recursion
            print(f"Error in RabbitMQ log handler: {e}")


# Patch existing log functions in other modules
def patch_module_log_function(module_name, function_name, service_name):
    """Patch log_message function in other modules"""
    try:
        # Try to import the module
        module = __import__(module_name, fromlist=['*'])
        original_function = getattr(module, function_name)
        
        # Create wrapper function
        def log_wrapper(message):
            # Call the original function
            result = original_function(message)
            
            # Determine status from message content
            if "error" in message.lower() or "failed" in message.lower():
                status = "ERROR"
            elif "warning" in message.lower():
                status = "WARNING"
            else:
                status = "INFO"
                
            # Send to RabbitMQ
            send_log_to_queue(service_name, status, f"LOG_{service_name}", message)
            
            return result
            
        # Replace the original function
        setattr(module, function_name, log_wrapper)
        print_only(f"Patched {module_name}.{function_name}")
        return True
        
    except Exception as e:
        print_only(f"Failed to patch {module_name}.{function_name}: {e}")
        return False


class RabbitMQLogStarter(models.AbstractModel):
    _name = 'rabbitmq.log.starter'
    _description = 'RabbitMQ Log Starter'
    
    @api.model
    def _register_hook(self):
        """Start the logging integration when Odoo starts"""
        global log_thread
        
        print_only("Setting up RabbitMQ logging")
        
        # Start log sender thread if not running
        if log_thread is None or not log_thread.is_alive():
            log_thread = threading.Thread(target=log_sender_thread, daemon=True)
            log_thread.start()
        
        # Add log handler to root logger
        handler = RabbitMQLogHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)  # Capture INFO and above
        logging.getLogger().addHandler(handler)
        print_only("Added RabbitMQ log handler to root logger")
        
        # Patch log functions in your existing modules
        modules_to_patch = [
            ('odoo.addons.rabbitmq_heartbeat.models.heartbeat', 'log_message', 'HEARTBEAT'),
            ('odoo.addons.rabbitmq_orders.models.pos_order', 'log_message', 'ORDERS'),
            ('odoo.addons.user_create.models.consumer_user_create', 'log_message', 'USER_CREATE'),
            ('odoo.addons.user_create.models.publisher_user_create', 'log_message', 'USER_CREATE'),
            ('odoo.addons.user_delete.models.rabbitmq_consumer', 'log_message', 'USER_DELETE'),
            ('odoo.addons.user_delete.models.rabbitmq_publisher', 'log_message', 'USER_DELETE'),
            ('odoo.addons.user_update.models.rabbitmq_consumer', 'log_message', 'USER_UPDATE'),
            ('odoo.addons.user_update.models.publisher_user_update', 'log_message', 'USER_UPDATE')
        ]
        
        # Try each module
        for module_path, function_name, service_name in modules_to_patch:
            patch_module_log_function(module_path, function_name, service_name)
        
        print_only("RabbitMQ logging setup complete")
        return super(RabbitMQLogStarter, self)._register_hook()