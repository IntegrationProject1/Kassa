import pika
import threading
import time
import datetime
import xml.etree.ElementTree as ET
import logging
import traceback
import os
from odoo import models, api, fields, _
from odoo.exceptions import UserError
from lxml import etree
import base64

_logger = logging.getLogger(__name__)

# Constants with environment variable fallbacks
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT'))
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASS')

# Define which queues we want to consume
SERVICE_QUEUES = [
    'crm_user_create',
    'facturatie_user_create',
    'frontend_user_create'
]

# Add this to make logs more visible
def log_message(message):
    print(f"[USER_CREATE_MODULE] {message}")
    _logger.info(message)

# XSD Schema as a constant
XSD_SCHEMA = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="UserMessage">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="ActionType" type="xs:string"/>
                <xs:element name="UserID" type="xs:string"/>
                <xs:element name="TimeOfAction" type="xs:dateTime"/>
                <xs:element name="Password" type="xs:string" minOccurs="0"/>
                <xs:element name="FirstName" type="xs:string" minOccurs="0"/>
                <xs:element name="LastName" type="xs:string" minOccurs="0"/>
                <xs:element name="PhoneNumber" type="xs:string" minOccurs="0"/>
                <xs:element name="EmailAddress" type="xs:string" minOccurs="0"/>
                <xs:element name="Business" minOccurs="0">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="BusinessName" type="xs:string" minOccurs="0"/>
                            <xs:element name="BusinessEmail" type="xs:string" minOccurs="0"/>
                            <xs:element name="RealAddress" type="xs:string" minOccurs="0"/>
                            <xs:element name="BTWNumber" type="xs:string" minOccurs="0"/>
                            <xs:element name="FacturationAddress" type="xs:string" minOccurs="0"/>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>'''

class UserCreateThread(threading.Thread):
    """Thread that listens for user create messages on RabbitMQ"""
    
    def __init__(self, env):
        super().__init__()
        self.env = env
        self.daemon = True  # Ensures thread stops when Odoo stops
        self.running = True
        self._cr = None
        self.connections = {}  # Storage for connections per queue
    
    def run(self):
        """Listens to messages from the specified service queues"""
        log_message(f"Starting UserCreateThread connecting to {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        log_message(f"Will consume from {len(SERVICE_QUEUES)} queues: {', '.join(SERVICE_QUEUES)}")
        
        while self.running:
            try:
                # Setup credentials
                credentials = pika.PlainCredentials(
                    username=RABBITMQ_USER,
                    password=RABBITMQ_PASSWORD
                )
                
                # Connect to RabbitMQ once
                log_message(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=RABBITMQ_HOST,
                        port=RABBITMQ_PORT,
                        credentials=credentials,
                        heartbeat=600
                    )
                )
                
                # Create a channel for each queue
                channels = {}
                consumers = {}
                
                for queue_name in SERVICE_QUEUES:
                    try:
                        log_message(f"Setting up consumer for queue: {queue_name}")
                        
                        # New channel for each queue
                        channels[queue_name] = connection.channel()
                        channel = channels[queue_name]
                        
                        # Declare the queue
                        channel.queue_declare(queue=queue_name, durable=True)
                        
                        # Check waiting messages
                        queue_info = channel.queue_declare(queue=queue_name, durable=True)
                        message_count = queue_info.method.message_count
                        log_message(f"Queue '{queue_name}' has {message_count} messages waiting")
                        
                        # Define callback specific for this queue
                        def make_callback(queue):
                            def callback(ch, method, properties, body):
                                try:
                                    log_message(f"Received message from {queue}: {body[:100]}...")
                                    success = self._process_message(body, queue)
                                    
                                    if success:
                                        ch.basic_ack(delivery_tag=method.delivery_tag)
                                        log_message(f"Message from {queue} processed successfully")
                                    else:
                                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                                        log_message(f"Message from {queue} processing failed")
                                except Exception as e:
                                    log_message(f"Error processing message from {queue}: {str(e)}")
                                    log_message(traceback.format_exc())
                                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                            return callback
                        
                        # Set up the consumer for this queue
                        channel.basic_qos(prefetch_count=1)
                        consumer_tag = channel.basic_consume(
                            queue=queue_name, 
                            on_message_callback=make_callback(queue_name)
                        )
                        consumers[queue_name] = consumer_tag
                        
                        log_message(f"Now consuming from queue: {queue_name}")
                        
                    except Exception as queue_error:
                        log_message(f"Error setting up consumer for queue {queue_name}: {str(queue_error)}")
                
                # Process data events for all channels
                while self.running and connection.is_open:
                    try:
                        # Process events for all open channels
                        connection.process_data_events(time_limit=1)
                        time.sleep(0.1)
                    except Exception as process_error:
                        log_message(f"Error processing events: {str(process_error)}")
                        if not connection.is_open:
                            break
                
            except Exception as e:
                log_message(f"RabbitMQ connection error: {str(e)}")
                log_message(traceback.format_exc())
                # Wait before retrying
                time.sleep(10)
            finally:
                # Close the connection if it's still open
                if 'connection' in locals() and connection and connection.is_open:
                    try:
                        connection.close()
                        log_message("RabbitMQ connection closed")
                    except:
                        pass
        
        log_message("UserCreateThread stopped")
    
    def _process_message(self, body, queue_name=None):
        """Processes an XML message to update or create a user"""
        try:
            # Log where the message came from
            source_info = f" from queue {queue_name}" if queue_name else ""
            log_message(f"Processing message{source_info}")
            
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
                    # Parse the user data from the XML
                    user_data = self._parse_user_data(xml_doc)
                    if not user_data:
                        log_message("Failed to parse user data from XML")
                        return False
                    
                    # Process the user data (create/update/delete)
                    success = self._process_user_data(user_data, env)
                    
                    if success:
                        new_cr.commit()
                        log_message("User update successfully committed")
                        return True
                    else:
                        new_cr.rollback()
                        log_message("User update failed, rolling back")
                        return False
                        
                except Exception as e:
                    new_cr.rollback()
                    log_message(f"Error processing user data: {str(e)}")
                    log_message(traceback.format_exc())
                    return False
                
        except Exception as e:
            log_message(f"Error in _process_message: {str(e)}")
            log_message(traceback.format_exc())
            return False
    
    def _parse_user_data(self, xml_doc):
        """Parse the XML and extract user data"""
        try:
            user_data = {}
            
            # Extract basic user information
            action_type_elem = xml_doc.find('.//ActionType')
            user_id_elem = xml_doc.find('.//UserID')
            time_of_action_elem = xml_doc.find('.//TimeOfAction')
            
            if action_type_elem is None or user_id_elem is None or time_of_action_elem is None:
                log_message("Required elements missing from XML")
                return None
                
            user_data['action_type'] = action_type_elem.text
            user_data['user_id'] = user_id_elem.text
            user_data['time_of_action'] = time_of_action_elem.text
            
            log_message(f"Basic user data: ActionType={user_data['action_type']}, UserID={user_data['user_id']}")
            
            # Optional password field
            password_elem = xml_doc.find('.//Password')
            if password_elem is not None and password_elem.text:
                user_data['password'] = password_elem.text
                log_message("Found Password field")
            else:
                user_data['password'] = ""  # Set password to empty string if not provided
            
            # Extract optional personal information
            optional_fields = ['FirstName', 'LastName', 'PhoneNumber', 'EmailAddress']
            for field in optional_fields:
                element = xml_doc.find(f'.//{field}')
                if element is not None and element.text:
                    # Convert XML field name to Odoo field name (camelCase to snake_case)
                    odoo_field = ''.join(['_' + c.lower() if c.isupper() else c for c in field]).lstrip('_')
                    user_data[odoo_field] = element.text
                    log_message(f"Found {field}: {element.text}")
            
            # Extract business information if present
            business_elem = xml_doc.find('.//Business')
            if business_elem is not None:
                log_message("Found Business element")
                business_data = {}
                business_fields = [
                    'BusinessName', 'BusinessEmail', 'RealAddress', 
                    'BTWNumber', 'FacturationAddress'
                ]
                for field in business_fields:
                    element = business_elem.find(f'.//{field}')
                    if element is not None and element.text:
                        odoo_field = ''.join(['_' + c.lower() if c.isupper() else c for c in field]).lstrip('_')
                        business_data[odoo_field] = element.text
                        log_message(f"Found Business.{field}: {element.text}")
                
                if business_data:
                    user_data['business'] = business_data
            
            return user_data
            
        except Exception as e:
            log_message(f"Error parsing user data: {str(e)}")
            log_message(traceback.format_exc())
            return None
    
    def _process_user_data(self, user_data, env):
        """Process the user data and update/create the user in Odoo"""
        try:
            user_model = env['res.users'].sudo()
            partner_model = env['res.partner'].sudo()

            user_id = user_data.get('user_id')
            log_message(f"Looking for user with ID/login: {user_id}")

            if user_data.get('action_type') == 'CREATE':
                log_message(f"Creating new user with ID/login: {user_id}")

                # Prepare values for creating a new user
                create_vals = {
                    'login': user_data.get('email_address'),
                    'name': f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}",
                    'email': user_data.get('email_address'),
                    'password': user_data.get('password'),
                }

                # Create the new user
                try:
                    new_user = user_model.create(create_vals)
                    log_message(f"Created new user: {new_user.id}, Login: {new_user.login}")

                    # Create the partner record
                    partner_vals = {
                        'name': f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}",
                        'email': user_data.get('email_address'),
                        'phone': user_data.get('phone_number'),
                    }

                    # Create the partner (individual or company based on the business data)
                    if 'business' in user_data:
                        business = user_data.get('business')
                        if 'business_name' in business:
                            partner_vals['company_name'] = business['business_name']
                        if 'real_address' in business:
                            partner_vals['street'] = business['real_address']
                        if 'btw_number' in business:
                            partner_vals['vat'] = business['btw_number']
                        if 'facturation_address' in business:
                            partner_vals['street2'] = business['facturation_address']

                    new_partner = partner_model.create(partner_vals)
                    log_message(f"Created new partner: {new_partner.id}, Name: {new_partner.name}")

                    # Link user to partner
                    new_user.partner_id = new_partner.id
                    log_message(f"Linked new user {new_user.id} to partner {new_partner.id}")

                    # Commit changes
                    new_user.env.cr.commit()

                    return True  # Success in creating user

                except Exception as e:
                    log_message(f"Error creating user or partner: {str(e)}")
                    log_message(traceback.format_exc())
                    return False

            elif user_data.get('action_type') == 'UPDATE':
                log_message(f"UPDATE action skipped for user ID {user_id}")
                return False  # Skip update action

            elif user_data.get('action_type') == 'DELETE':
                # Handle deletion or archiving of user (as per existing logic)
                return False

            else:
                log_message(f"Unknown action type: {user_data.get('action_type')}")
                return False

        except Exception as e:
            log_message(f"Unexpected error processing user data: {str(e)}")
            log_message(traceback.format_exc())
            return False


# Global thread instance
user_create_thread = None

class RabbitMQUserCreate(models.AbstractModel):
    _name = 'rabbitmq.user.create'
    _description = 'RabbitMQ User create Service'
    
    @api.model
    def start_service(self):
        """Start the user create service if it's not already running"""
        global user_create_thread
        if not user_create_thread or not user_create_thread.is_alive():
            print("Starting RabbitMQ User create Service...")
            user_create_thread = UserCreateThread(self.env)
            user_create_thread.start()
            return True
        print("RabbitMQ User create Service already running.")
        return False
    
    @api.model
    def stop_service(self):
        """Stop the user create service"""
        global user_create_thread
        if user_create_thread and user_create_thread.is_alive():
            user_create_thread.stop()
            return True
        return False

class RabbitMQUserCreateStartup(models.AbstractModel):
    _name = "rabbitmq.user.create.startup"
    _description = "Start RabbitMQ User create on Odoo startup"
    
    @api.model
    def _register_hook(self):
        """Start the service on Odoo startup"""
        self.env['rabbitmq.user.create'].start_service()
        
        
"""om te testen of het werkt kan je dit in rabbitmq zetten (gegevens wel aanpassen): 
<UserMessage>
    <ActionType>CREATE</ActionType>
    <UserID>54321</UserID>
    <TimeOfAction>2025-03-30T12:34:56Z</TimeOfAction>
    <FirstName>John</FirstName>
    <LastName>Pork</LastName>
    <PhoneNumber>+1234563890</PhoneNumber>
    <EmailAddress>john.pork@example.com</EmailAddress>
</UserMessage>
"""