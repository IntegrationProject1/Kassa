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
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')

# Define which queues we want to consume
SERVICE_QUEUES = [
    'kassa_user_update',
    # 'crm_user_update',
    # 'facturatie_user_update',
    # 'frontend_user_update'
]

# Add this to make logs more visible
def log_message(message):
    print(f"[CUSTOMER_UPDATE_MODULE] {message}")
    _logger.info(message)

# XSD Schema as a constant
XSD_SCHEMA = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="UserMessage">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="ActionType" type="xs:string"/>
                <xs:element name="UUID" type="xs:dateTime"/>
                <xs:element name="TimeOfAction" type="xs:dateTime"/>
                <xs:element name="EncryptedPassword" type="xs:string" minOccurs="0"/>
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

class CustomerUpdateThread(threading.Thread):
    """Thread that listens for customer update messages on RabbitMQ"""
    
    def __init__(self, env):
        super().__init__()
        self.env = env
        self.daemon = True  # Ensures thread stops when Odoo stops
        self.running = True
        self._cr = None
        self.connections = {}  # Storage for connections per queue
    
    def run(self):
        """Listens to messages from the specified service queues"""
        log_message(f"Starting CustomerUpdateThread connecting to {RABBITMQ_HOST}:{RABBITMQ_PORT}")
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
        
        log_message("CustomerUpdateThread stopped")
    
    def _process_message(self, body, queue_name=None):
        """Processes an XML message to update or create a customer"""
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
                    # Parse the customer data from the XML
                    customer_data = self._parse_customer_data(xml_doc)
                    if not customer_data:
                        log_message("Failed to parse customer data from XML")
                        return False
                    
                    # Process the customer data (create/update/delete)
                    success = self._process_customer_data(customer_data, env)
                    
                    if success:
                        new_cr.commit()
                        log_message("Customer update successfully committed")
                        return True
                    else:
                        new_cr.rollback()
                        log_message("Customer update failed, rolling back")
                        return False
                        
                except Exception as e:
                    new_cr.rollback()
                    log_message(f"Error processing customer data: {str(e)}")
                    log_message(traceback.format_exc())
                    return False
                
        except Exception as e:
            log_message(f"Error in _process_message: {str(e)}")
            log_message(traceback.format_exc())
            return False
    
    def _parse_customer_data(self, xml_doc):
        """Parse the XML and extract customer data"""
        try:
            customer_data = {}
            
            # Extract basic customer information (using same element names from XSD)
            action_type_elem = xml_doc.find('.//ActionType')
            uuid_elem = xml_doc.find('.//UUID')  # Changed from UserID to UUID
            time_of_action_elem = xml_doc.find('.//TimeOfAction')
            
            if action_type_elem is None or uuid_elem is None or time_of_action_elem is None:
                log_message("Required elements missing from XML")
                return None
                
            customer_data['action_type'] = action_type_elem.text

            # Store UUID as customer_id (now as a timestamp string)
            try:
                # Store it as a string directly - no need to convert to int
                customer_data['customer_id'] = uuid_elem.text
                log_message(f"Parsed UUID timestamp: {customer_data['customer_id']}")
            except (ValueError, TypeError):
                log_message(f"Error: UUID must be a valid dateTime, received: {uuid_elem.text}")
                return None
            
            customer_data['time_of_action'] = time_of_action_elem.text
            
            log_message(f"Basic customer data: ActionType={customer_data['action_type']}, UUID={customer_data['customer_id']}")
            
            # Extract optional personal information
            optional_fields = ['FirstName', 'LastName', 'PhoneNumber', 'EmailAddress']
            for field in optional_fields:
                element = xml_doc.find(f'.//{field}')
                if element is not None and element.text:
                    # Convert XML field name to Odoo field name (camelCase to snake_case)
                    odoo_field = ''.join(['_' + c.lower() if c.isupper() else c for c in field]).lstrip('_')
                    customer_data[odoo_field] = element.text
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
                    customer_data['business'] = business_data
            
            return customer_data
            
        except Exception as e:
            log_message(f"Error parsing customer data: {str(e)}")
            log_message(traceback.format_exc())
            return None
    
    def _process_customer_data(self, customer_data, env):
        """Process the customer data and update/create the customer in Odoo"""
        try:
            partner_model = env['res.partner'].sudo()
            
            # Log the first 10 customers in the system for debugging
            all_customers = partner_model.search([('customer_rank', '>', 0)], limit=10)
            log_message(f"First 10 customers in the system:")
            for i, customer in enumerate(all_customers):
                log_message(f"  Customer {i+1}: ID={customer.id}, Name={customer.name}, Email={customer.email}, External ID={customer.external_id}")
            
            # Update the customer lookup code in _process_customer_data
            customer_id = customer_data.get('customer_id')
            log_message(f"Looking for customer with UUID (external_id): {customer_id}")

            # First, try to find by external_id (highest priority)
            customer = partner_model.search([
                ('external_id', '=', customer_id)
            ], limit=1)
            
            if customer:
                log_message(f"Found customer by external_id={customer_id}: ID={customer.id}, Name={customer.name}")
            else:
                log_message(f"No customer found with external_id={customer_id}")
            
            if customer_data.get('action_type') == 'UPDATE':
                if not customer:
                    log_message(f"Customer with UUID {customer_id} not found for update")
                    return False
                    
                log_message(f"Updating customer with UUID {customer_id}, database ID: {customer.id}, external_id: {customer.external_id}")
                
                # Always ensure external_id is set when updating by it
                update_vals = {}
                if not customer.external_id:
                    update_vals['external_id'] = customer_id
                    log_message(f"Setting missing external_id to {customer_id}")
                    
                if 'first_name' in customer_data or 'last_name' in customer_data:
                    first_name = customer_data.get('first_name', '')
                    last_name = customer_data.get('last_name', '')
                    
                    # If either is provided, create full name
                    if first_name or last_name:
                        # If both provided, use both
                        if first_name and last_name:
                            update_vals['name'] = f"{first_name} {last_name}"
                        # If only first name provided
                        elif first_name:
                            update_vals['name'] = first_name
                        # If only last name provided
                        else:
                            update_vals['name'] = last_name
                        
                        log_message(f"Updating customer name to: {update_vals['name']}")
                
                if 'email_address' in customer_data:
                    update_vals['email'] = customer_data.get('email_address')
                    log_message(f"Updating customer email to: {update_vals['email']}")
                    
                if 'phone_number' in customer_data:
                    update_vals['phone'] = customer_data.get('phone_number')
                    log_message(f"Updating customer phone to: {update_vals['phone']}")
                    
                # Update business information
                if 'business' in customer_data:
                    business = customer_data.get('business')
                    log_message(f"Processing business data: {business}")
                    
                    # Check if customer is a company or has a parent company
                    has_company = customer.company_name or customer.parent_id
                    log_message(f"Customer has company: {has_company}")
                    
                    # If business name exists in data, process business info
                    if 'business_name' in business:
                        business_name = business.get('business_name')
                        log_message(f"Business name from message: {business_name}")
                        
                        if has_company:
                            # Update existing company info
                            if customer.is_company:
                                update_vals['name'] = business_name
                                log_message(f"Updating company name to: {business_name}")
                            else:
                                # This is a contact under a company, update company_name field
                                update_vals['company_name'] = business_name
                                log_message(f"Updating company_name field to: {business_name}")
                        else:
                            # Create new company
                            log_message(f"Creating new company: {business_name}")
                            try:
                                # Create a new company partner
                                company_partner_vals = {
                                    'name': business_name,
                                    'is_company': True,
                                    'type': 'contact',
                                    'customer_rank': 1,  # Mark as customer
                                }
                                
                                # Add business fields if available
                                if 'business_email' in business:
                                    company_partner_vals['email'] = business.get('business_email')
                                    log_message(f"Setting company email: {company_partner_vals['email']}")
                                    
                                if 'real_address' in business:
                                    company_partner_vals['street'] = business.get('real_address')
                                    log_message(f"Setting company address: {company_partner_vals['street']}")
                                    
                                if 'btw_number' in business:
                                    company_partner_vals['vat'] = business.get('btw_number')
                                    log_message(f"Setting company VAT: {company_partner_vals['vat']}")
                                    
                                if 'facturation_address' in business:
                                    company_partner_vals['street2'] = business.get('facturation_address')
                                    log_message(f"Setting company facturation address: {company_partner_vals['street2']}")
                                
                                # Create the company partner
                                new_company_partner = partner_model.create(company_partner_vals)
                                log_message(f"Created new company partner with ID: {new_company_partner.id}")
                                
                                # Link individual to the company
                                update_vals['parent_id'] = new_company_partner.id
                                log_message(f"Linking customer to company with ID: {new_company_partner.id}")
                                
                                # Set the contact type to "contact" (individual)
                                update_vals['type'] = 'contact'
                                
                            except Exception as e:
                                log_message(f"Error creating company: {str(e)}")
                                log_message(traceback.format_exc())
                                return False
                    else:
                        # No business name but other business fields
                        if 'business_email' in business:
                            update_vals['email'] = business.get('business_email')
                            log_message(f"Updating business email to: {update_vals['email']}")
                            
                        if 'real_address' in business:
                            update_vals['street'] = business.get('real_address')
                            log_message(f"Updating address to: {update_vals['street']}")
                        
                        if 'btw_number' in business:
                            update_vals['vat'] = business.get('btw_number')
                            log_message(f"Updating VAT to: {update_vals['vat']}")
                        
                        if 'facturation_address' in business:
                            update_vals['street2'] = business.get('facturation_address')
                            log_message(f"Updating facturation address to: {update_vals['street2']}")
                    
                if update_vals:
                    log_message(f"Writing customer fields: {update_vals.keys()}")
                    try:
                        customer.write(update_vals)
                        log_message("Customer fields updated successfully")
                    except Exception as e:
                        log_message(f"Error updating customer fields: {str(e)}")
                        log_message(traceback.format_exc())
                        return False
                    
                log_message(f"Customer {customer_id} updated successfully")
                return True
                    
            elif customer_data.get('action_type') == 'CREATE':
                # Skip CREATE actions as they are handled by another module
                log_message(f"CREATE action for customer ID {customer_id} skipped - handled by another module")
                return True  # Return True to acknowledge the message
                    
            elif customer_data.get('action_type') == 'DELETE':
                # Skip DELETE actions as they are handled by another module
                log_message(f"DELETE action for customer ID {customer_id} skipped - handled by another module")
                return True
                
            else:
                log_message(f"Unknown action type: {customer_data.get('action_type')}")
                return False
                
        except Exception as e:
            log_message(f"Unexpected error processing customer data: {str(e)}")
            log_message(traceback.format_exc())
            return False
    
    def stop(self):
        """Stop the thread gracefully"""
        self.running = False
        print("Stopping CustomerUpdateThread...")

# Global thread instance
customer_update_thread = None

class RabbitMQCustomerUpdate(models.AbstractModel):
    _name = 'rabbitmq.customer.update'
    _description = 'RabbitMQ Customer Update Service'
    
    @api.model
    def start_service(self):
        """Start the customer update service if it's not already running"""
        global customer_update_thread
        if not customer_update_thread or not customer_update_thread.is_alive():
            print("Starting RabbitMQ Customer Update Service...")
            customer_update_thread = CustomerUpdateThread(self.env)
            customer_update_thread.start()
            return True
        print("RabbitMQ Customer Update Service already running.")
        return False
    
    @api.model
    def stop_service(self):
        """Stop the customer update service"""
        global customer_update_thread
        if customer_update_thread and customer_update_thread.is_alive():
            customer_update_thread.stop()
            return True
        return False

class RabbitMQCustomerUpdateStartup(models.AbstractModel):
    _name = "rabbitmq.customer.update.startup"
    _description = "Start RabbitMQ Customer Update on Odoo startup"
    
    @api.model
    def _register_hook(self):
        """Start the service on Odoo startup"""
        self.env['rabbitmq.customer.update'].start_service()