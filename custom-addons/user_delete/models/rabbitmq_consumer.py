import pika
import threading
import time
import datetime
import xml.etree.ElementTree as ET
import logging
import traceback
from odoo import models, api, fields, _
from odoo.exceptions import UserError
from lxml import etree
from io import StringIO

_logger = logging.getLogger(__name__)

# Constanten
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 30020
RABBITMQ_USER = "ehbstudent"
RABBITMQ_PASSWORD = "wpqjf9mI3DKZdZDaa!"
QUEUE_NAME = "kassa_user_delete"

# Add XSD schema as a constant
XSD_SCHEMA = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="UserMessage">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="ActionType" type="xs:string"/>
                <xs:element name="UserID" type="xs:string"/>
                <xs:element name="TimeOfAction" type="xs:dateTime"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
'''

# Add this to make logs more visible
def log_message(message):
    print(f"[USER_DELETE_MODULE] {message}")
    _logger.info(message)

class UserDeleteThread(threading.Thread):
    """Thread die luistert naar user delete berichten op RabbitMQ."""
    
    def __init__(self, env):
        super().__init__()
        self.env = env
        self.daemon = True  # Zorgt ervoor dat de thread stopt als Odoo stopt
        self.running = True
        self._cr = None
    
    def run(self):
        """Luistert naar berichten van de kassa_user_delete queue."""
        log_message(f"Starting UserDeleteThread connecting to {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        
        while self.running:
            connection = None
            try:
                # Setup credentials
                credentials = pika.PlainCredentials(
                    username=RABBITMQ_USER,
                    password=RABBITMQ_PASSWORD
                )
                
                # Connect to RabbitMQ
                log_message(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=RABBITMQ_HOST,
                        port=RABBITMQ_PORT,
                        credentials=credentials,
                        heartbeat=600
                    )
                )
                channel = connection.channel()
                
                # Declare the queue
                channel.queue_declare(queue=QUEUE_NAME, durable=True)
                
                # Get message count
                queue_info = channel.queue_declare(queue=QUEUE_NAME, durable=True)
                message_count = queue_info.method.message_count
                log_message(f"Queue '{QUEUE_NAME}' has {message_count} messages waiting")
                _logger.info(f"Queue '{QUEUE_NAME}' has {message_count} messages waiting")
                
                def callback(ch, method, properties, body):
                    try:
                        log_message(f"Received message: {body[:100]}...")  # Log first 100 chars
                        _logger.info(f"Received message: {body[:100]}...")
                        success = self._process_message(body)
                        
                        if success:
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            log_message("Message processing successful and acknowledged")
                        else:
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                            log_message("Message processing failed, rejecting message")
                    except Exception as e:
                        log_message(f"Error processing message: {str(e)}")
                        log_message(traceback.format_exc())
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                
                # Set up consumer
                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
                
                log_message(f"Now consuming from queue: {QUEUE_NAME}")
                
                # Process messages but check periodically if we should stop
                while self.running:
                    connection.process_data_events(time_limit=1)
                    time.sleep(0.1)
                
            except Exception as e:
                log_message(f"RabbitMQ connection error: {str(e)}")
                log_message(traceback.format_exc())
                # Wait before retrying
                time.sleep(10)
            finally:
                if connection and connection.is_open:
                    try:
                        connection.close()
                        log_message("RabbitMQ connection closed")
                    except:
                        pass
        
        log_message("UserDeleteThread stopped")
    
    def _process_message(self, body):
        """Verwerk een XML bericht om een gebruiker te verwijderen."""
        try:
            # Parse the XML message
            message_str = body.decode('utf-8')
            log_message(f"Processing XML message: {message_str}")
            
            # Validate against XSD schema
            try:
                # Parse XSD schema and create validator
                schema_doc = etree.fromstring(XSD_SCHEMA.encode('utf-8'))
                schema = etree.XMLSchema(schema_doc)
                
                # Parse message
                xml_doc = etree.fromstring(message_str.encode('utf-8'))
                
                # Validate
                if not schema.validate(xml_doc):
                    validation_errors = schema.error_log
                    log_message(f"XML validation errors: {validation_errors}")
                    return False
                    
                log_message("XML message validated successfully against schema")
                
            except etree.XMLSyntaxError as xml_err:
                log_message(f"XML syntax error: {str(xml_err)}")
                return False
            except Exception as validate_err:
                log_message(f"XML validation error: {str(validate_err)}")
                return False
            
            # Create a new environment with a new cursor
            registry = self.env.registry
            with registry.cursor() as new_cr:
                # Create an environment with the same uid and context but new cursor
                env = api.Environment(new_cr, self.env.uid, self.env.context)
                
                try:
                    # Use standard ElementTree for further processing as before
                    root = ET.fromstring(message_str)
                    
                    # Debug the root to ensure it parsed correctly
                    log_message(f"XML Root tag: {root.tag}")
                    log_message(f"Root children: {[child.tag for child in root]}")
                    
                    # Check message format
                    action_type = root.find('ActionType')
                    user_id = root.find('UserID')
                    time_of_action = root.find('TimeOfAction')
                    
                    # Debug logging with element existence check
                    if action_type is not None:
                        log_message(f"Found ActionType: '{action_type.text}'")
                    else:
                        log_message("ActionType element not found")
                        return False
                        
                    if user_id is not None:
                        log_message(f"Found UserID: '{user_id.text}'")
                    else:
                        log_message("UserID element not found")
                        return False
                    
                    if time_of_action is not None:
                        log_message(f"Found TimeOfAction: '{time_of_action.text}'")
                    else:
                        log_message("TimeOfAction element not found")
                        return False
                    
                    # Check if elements have text content
                    if not action_type.text or action_type.text.strip() == '':
                        log_message("ActionType element has no text")
                        return False
                    if not user_id.text or user_id.text.strip() == '':
                        log_message("UserID element has no text")
                        return False
                    if not time_of_action.text or time_of_action.text.strip() == '':
                        log_message("TimeOfAction element has no text")
                        return False
                    
                    # Check if action is DELETE (with case and whitespace handling)
                    if action_type.text.strip().upper() != 'DELETE':
                        log_message(f"Not a DELETE action: '{action_type.text}'")
                        return False
                    
                    user_id_value = user_id.text.strip()
                    log_message(f"Processing delete request for user ID: {user_id_value}")
                    
                    # Find the user - search for numeric ID or login (email)
                    # Convert to integer if it's a number
                    try:
                        numeric_id = int(user_id_value)
                        log_message(f"Converted user ID to numeric: {numeric_id}")
                    except (ValueError, TypeError):
                        numeric_id = -1
                        log_message(f"User ID is not numeric, using -1 for numeric search")
                        
                    log_message(f"Searching for user with ID {numeric_id} or login {user_id_value}")
                    
                    # Diagnostic - verify users exist in database
                    all_users = env['res.users'].sudo().search_read([('id', '>', 0)], ['id', 'name', 'login'])
                    log_message(f"Found {len(all_users)} users in database. First few: {all_users[:10]}")
                    
                    user = env['res.users'].sudo().search([
                        '|', 
                        ('id', '=', numeric_id),
                        ('login', '=', user_id_value)
                    ], limit=1)
                    
                    if not user:
                        log_message(f"User not found for ID/login: {user_id_value}")
                        return False
                    
                    # Don't delete admin users
                    if user.id <= 2:  # Also protect admin (2)
                        log_message(f"Cannot delete system user with ID: {user.id}")
                        return False
                    
                    # Store user info before deletion
                    user_name = user.name
                    user_id = user.id
                    user_login = user.login
                    
                    # Log the user deletion
                    log_message(f"Deleting user: {user_name} (ID: {user_id}, Login: {user_login})")
                    
                    try:
                        # First archive the user
                        user.write({'active': False})
                        log_message(f"User {user_name} archived successfully")
                        
                        # Then try to delete
                        user.unlink()
                        log_message(f"User with ID {user_id} deleted successfully")
                        
                        new_cr.commit()
                        log_message("Database transaction committed")
                        return True
                    except Exception as delete_error:
                        log_message(f"Error during user deletion: {str(delete_error)}")
                        log_message(traceback.format_exc())
                        new_cr.rollback()
                        log_message("Rolling back transaction")
                        return False
                        
                except Exception as e:
                    new_cr.rollback()
                    log_message(f"Error processing user deletion: {str(e)}")
                    log_message(traceback.format_exc())
                    return False
        except Exception as e:
            log_message(f"Error in _process_message: {str(e)}")
            log_message(traceback.format_exc())
            return False
    
    def stop(self):
        """Stop de thread netjes."""
        self.running = False
        print("Stopping UserDeleteThread...")

# Globale thread instance
user_delete_thread = None

class RabbitMQUserDelete(models.AbstractModel):
    _name = 'rabbitmq.user.delete'
    _description = 'RabbitMQ User Delete Service'
    
    @api.model
    def start_service(self):
        """Start de user delete service als deze nog niet loopt."""
        global user_delete_thread
        if not user_delete_thread or not user_delete_thread.is_alive():
            print("Starting RabbitMQ User Delete Service...")
            user_delete_thread = UserDeleteThread(self.env)
            user_delete_thread.start()
            return True
        print("RabbitMQ User Delete Service already running.")
        return False
    
    @api.model
    def stop_service(self):
        """Stop de user delete service."""
        global user_delete_thread
        if user_delete_thread and user_delete_thread.is_alive():
            user_delete_thread.stop()
            return True
        return False
    
    @api.model
    def test_connection(self):
        """Test de verbinding met RabbitMQ."""
        try:
            # Setup credentials
            credentials = pika.PlainCredentials(
                username=RABBITMQ_USER,
                password=RABBITMQ_PASSWORD
            )
            
            # Connect to RabbitMQ
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials
                )
            )
            channel = connection.channel()
            
            # Check queue status
            queue_info = channel.queue_declare(queue=QUEUE_NAME, durable=True)
            message_count = queue_info.method.message_count
            
            # Close connection
            connection.close()
            
            print(f"RabbitMQ connection test successful: queue has {message_count} messages")
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Connection Successful'),
                    'message': _('Successfully connected to RabbitMQ. Queue %s has %s messages.') % (QUEUE_NAME, message_count),
                    'sticky': False,
                    'type': 'success'
                }
            }
        except Exception as e:
            print(f"RabbitMQ connection test failed: {str(e)}")
            print(traceback.format_exc())
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Connection Failed'),
                    'message': str(e),
                    'sticky': False,
                    'type': 'danger'
                }
            }

class RabbitMQUserDeleteStartup(models.AbstractModel):
    _name = "rabbitmq.user.delete.startup"
    _description = "Start RabbitMQ User Delete bij Odoo opstart"
    
    @api.model
    def _register_hook(self):
        """Start de service bij Odoo opstart."""
        self.env['rabbitmq.user.delete'].start_service()