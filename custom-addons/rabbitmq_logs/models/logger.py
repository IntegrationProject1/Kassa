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
import sys
import queue
from odoo import models, api

# Add more descriptive prefix for clearer logs
DEBUG_PREFIX = "[RABBITMQ_LOGS_DEBUG]"
LOG_PREFIX = "[RABBITMQ_LOGS]"

_logger = logging.getLogger(__name__)

# RabbitMQ configuration
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'integrationproject-2425s2-001.westeurope.cloudapp.azure.com')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 30020)) 
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_EXCHANGE = 'log_monitoring'
RABBITMQ_QUEUE = 'controlroom.log.events'
ROUTING_KEY = 'controlroom.log.events'

# Add environment variable check with detailed output
def debug_print(message):
    """Print debug messages with timestamp and special prefix."""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print(f"{timestamp} {DEBUG_PREFIX} {message}", file=sys.stderr)

debug_print(f"Environment variables: RABBITMQ_HOST={RABBITMQ_HOST}, PORT={RABBITMQ_PORT}, USER={RABBITMQ_USER}, EXCHANGE={RABBITMQ_EXCHANGE}, QUEUE={RABBITMQ_QUEUE}")

# Original print_only function enhanced with timestamp
def print_only(message):
    """Print a message without sending it to the logger to avoid recursion."""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print(f"{timestamp} {LOG_PREFIX} {message}", file=sys.stderr)

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

# Global log queue and thread
log_queue = queue.Queue()
log_thread = None
running = False
log_schema = None

try:
    # Setup XML schema for validation - fix for Unicode encoding issue
    xsd_doc = etree.fromstring(LOG_XSD.encode('utf-8'))
    log_schema = etree.XMLSchema(xsd_doc)
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
        
    # Track seen logs for monitoring
    # if hasattr(periodic_log_test, "seen_logs"):
    #     log_key = f"{service_name}:{code}"
    #     periodic_log_test.seen_logs.add(log_key)
    
    # Truncate message if too long
    if message and len(message) > 2000:
        message = message[:1997] + "..."
        
    # Create XML message
    xml_message = create_log_message(service_name, status, code, message)
    
    # Optional validation
    if not validate_xml(xml_message):
        debug_print("Invalid XML log message generated, but will send anyway")
    
    # Add to queue
    log_queue.put(xml_message)

# Enhanced log sender thread with more debug output
def log_sender_thread():
    connection = None
    channel = None
    connected = False
    reconnect_delay = 5  # seconds
    
    debug_print("Log sender thread initializing")
    
    while True:
        # Connect to RabbitMQ if not connected
        if not connected:
            try:
                debug_print(f"Attempting to connect to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
                credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
                parameters = pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                
                # Declare exchange and queue
                debug_print(f"Setting up exchange '{RABBITMQ_EXCHANGE}' and queue '{RABBITMQ_QUEUE}'")
                channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type='direct', durable=True)
                channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
                channel.queue_bind(exchange=RABBITMQ_EXCHANGE, queue=RABBITMQ_QUEUE, routing_key='logs.events')
                
                connected = True
                debug_print("Successfully connected to RabbitMQ and set up channel")
                print_only("Connected to RabbitMQ and ready to send logs")
                reconnect_delay = 5  # Reset delay on successful connection
            except Exception as e:
                error_details = traceback.format_exc()
                print_only(f"Failed to connect to RabbitMQ: {e}")
                debug_print(f"Connection error details:\n{error_details}")
                print_only(f"Will retry connection in {reconnect_delay} seconds")
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)  # Exponential backoff with 60s max
                continue

        # Process log messages from queue
        try:
            # Try to get a message with a 1-second timeout
            try:
                log_message = log_queue.get(block=True, timeout=1.0)
                debug_print(f"Processing log message: {log_message[:100]}..." if len(log_message) > 100 else log_message)
                
                # Publish message to RabbitMQ
                channel.basic_publish(
                    exchange=RABBITMQ_EXCHANGE,
                    routing_key='logs.events',
                    body=log_message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                        content_type='application/xml'
                    )
                )
                debug_print("Log message successfully published to RabbitMQ")
                log_queue.task_done()
            except queue.Empty:
                # No message in queue, just continue
                pass
                
        except pika.exceptions.AMQPError as e:
            print_only(f"RabbitMQ connection lost: {e}")
            debug_print(f"AMQP error details: {type(e).__name__}: {str(e)}")
            connected = False
            
            # Close connection if it exists
            try:
                if connection and connection.is_open:
                    connection.close()
            except:
                pass
                
        except Exception as e:
            error_details = traceback.format_exc()
            print_only(f"Error in log sender thread: {e}")
            debug_print(f"Unexpected error details:\n{error_details}")

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
        
        print("=============================================")
        print("RABBITMQ_LOGS REGISTER HOOK CALLED")
        print("=============================================")
        
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

# debug logs

# def periodic_log_test():
#     """Send a test log message periodically to verify the logging system is working"""
#     try:
#         # Create and send a test log
#         message = f"RabbitMQ Logging System Test Message - {datetime.datetime.now().isoformat()}"
#         send_log_to_queue("RABBITMQ_LOGS_TEST", "INFO", "PERIODIC_TEST", message)
#         debug_print("Sent periodic test log message")
        
#         # Check if we've seen any logs from other modules
#         if hasattr(periodic_log_test, "seen_logs"):
#             elapsed = datetime.datetime.now() - periodic_log_test.last_check
#             if len(periodic_log_test.seen_logs) == periodic_log_test.previous_count and elapsed.total_seconds() > 60:
#                 # No new logs seen for a minute, try manual patching
#                 print_only("No new logs seen in 60 seconds, attempting to patch modules again")
#                 for module_path, function_name, service_name in [
#                     ('odoo.addons.rabbitmq_heartbeat.models.heartbeat', 'log_message', 'HEARTBEAT'),
#                     ('odoo.addons.user_create.models.consumer_user_create', 'log_message', 'USER_CREATE'),
#                     ('odoo.addons.user_delete.models.rabbitmq_consumer', 'log_message', 'USER_DELETE'),
#                 ]:
#                     patch_module_log_function(module_path, function_name, service_name)
            
#             periodic_log_test.previous_count = len(periodic_log_test.seen_logs)
#             periodic_log_test.last_check = datetime.datetime.now()
#     except Exception as e:
#         print_only(f"Error in periodic test: {e}")
    
#     # Schedule next run in 20 seconds
#     threading.Timer(20.0, periodic_log_test).start()

# Initialize monitoring data
# periodic_log_test.seen_logs = set()
# periodic_log_test.previous_count = 0
# periodic_log_test.last_check = datetime.datetime.now()

# Create class that can be imported and used from other modules
class RabbitMQLogService:
    @staticmethod
    def test_log(message):
        """Test the logging system with a manual message"""
        send_log_to_queue("MANUAL_TEST", "INFO", "TEST", message)
        return True
    
    @staticmethod
    def start_logging():
        """Manually start the logging system"""
        global log_thread, running
        
        print_only("Manually starting RabbitMQ logging system")
        running = True
        
        # Start log sender thread if not running
        if log_thread is None or not log_thread.is_alive():
            log_thread = threading.Thread(target=log_sender_thread, daemon=True)
            log_thread.start()
            
        # Add handler to root logger
        handler = RabbitMQLogHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(handler)
        
        # Patch common modules
        for module_path, function_name, service_name in [
            ('odoo.addons.rabbitmq_heartbeat.models.heartbeat', 'log_message', 'HEARTBEAT'),
            ('odoo.addons.rabbitmq_orders.models.pos_order', 'log_message', 'ORDERS'),
            ('odoo.addons.user_create.models.consumer_user_create', 'log_message', 'USER_CREATE'),
            ('odoo.addons.user_delete.models.rabbitmq_consumer', 'log_message', 'USER_DELETE'),
            ('odoo.addons.user_update.models.rabbitmq_consumer', 'log_message', 'USER_UPDATE'),
        ]:
            patch_module_log_function(module_path, function_name, service_name)
            
        # # Start periodic log tests
        # threading.Timer(5.0, periodic_log_test).start()
        
        return True

# Start the logging service automatically when module is loaded
def delayed_start():
    time.sleep(3)  # Short delay to let Odoo start
    log_service = RabbitMQLogService()
    log_service.start_logging()

threading.Thread(target=delayed_start, daemon=True).start()