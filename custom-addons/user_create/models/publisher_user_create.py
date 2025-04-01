import pika
import os
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from odoo import models, api, fields
from lxml import etree

_logger = logging.getLogger(__name__)

# RabbitMQ connection parameters
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT'))
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')

# Exchange and target queue definitions
EXCHANGE_NAME = 'user'
TARGET_QUEUES = [
    {'queue': 'crm_user_create', 'routing_key': 'crm.user.create'},
    {'queue': 'facturatie_user_create', 'routing_key': 'facturatie.user.create'},
    {'queue': 'frontend_user_create', 'routing_key': 'frontend.user.create'}
]

# XSD Schema for validation
USER_CREATE_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="UserMessage">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="ActionType" type="xs:string"/>
                <xs:element name="UserID" type="xs:string"/>
                <xs:element name="TimeOfAction" type="xs:dateTime"/>
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

def log_message(message):
    """Standard logging function"""
    print(f"[USER_CREATE_MODULE] {message}")
    _logger.info(message)

log_message("RabbitMQ User Create Publisher loaded")

class ResPartner(models.Model):
    _inherit = 'res.partner'
    
    def _get_rabbitmq_connection_params(self):
        """Get RabbitMQ connection parameters from environment variables"""
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        return pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )
    
    def validate_xml_against_xsd(self, xml_string, xsd_string):
        """Validate XML against XSD schema"""
        try:
            xml_doc = etree.fromstring(xml_string.encode('utf-8'))
            xsd_doc = etree.fromstring(xsd_string.encode('utf-8'))
            schema = etree.XMLSchema(xsd_doc)
            
            is_valid = schema.validate(xml_doc)
            if not is_valid:
                log_message(f"XML validation errors: {schema.error_log}")
                
            return is_valid
        except Exception as e:
            log_message(f"XML validation error: {e}")
            return False
    
    def create_user_create_message(self, partner_data):
        """Create XML message for user creation with support for nested elements"""
        # Create the root element
        root = ET.Element("UserMessage")
        
        # Add regular elements
        for key, value in partner_data.items():
            if key == 'Business':
                continue  # Handle business separately
            if value is not None and value != '':
                child = ET.SubElement(root, key)
                child.text = str(value)
        
        # Add business element if present
        if 'Business' in partner_data and partner_data['Business']:
            business_element = ET.SubElement(root, "Business")
            business_data = partner_data['Business']
            
            for bus_key, bus_value in business_data.items():
                if bus_value is not None and bus_value != '':
                    bus_child = ET.SubElement(business_element, bus_key)
                    bus_child.text = str(bus_value)
        
        # Convert to XML string
        xml_string = ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8')
        
        # Validate against XSD schema
        is_valid = self.validate_xml_against_xsd(xml_string, USER_CREATE_XSD)
        if not is_valid:
            log_message("Generated XML does not conform to XSD schema")
            
        return xml_string
    
    def publish_user_create(self, partner_data):
        """Publish user create message to other service queues"""
        try:
            user_id = partner_data.get('UserID')
            log_message(f"Publishing user create message for user_id: {user_id}")
            
            # Create the message
            message = self.create_user_create_message(partner_data)
            log_message(f"Message created successfully: {message}")
            
            # Connect to RabbitMQ
            log_message(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
            connection = pika.BlockingConnection(self._get_rabbitmq_connection_params())
            log_message("RabbitMQ connection established")
            
            channel = connection.channel()
            log_message("RabbitMQ channel created")
            
            # Ensure the exchange exists
            log_message(f"Declaring exchange '{EXCHANGE_NAME}'...")
            channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)
            log_message(f"Exchange '{EXCHANGE_NAME}' declared")
            
            # Publish to each target queue
            success_count = 0
            for target in TARGET_QUEUES:
                queue_name = target['queue']
                routing_key = target['routing_key']
                
                try:
                    # Ensure the queue exists
                    log_message(f"Declaring queue '{queue_name}'...")
                    channel.queue_declare(queue=queue_name, durable=True)
                    log_message(f"Queue '{queue_name}' declared")
                    
                    # Bind queue to exchange
                    log_message(f"Binding queue '{queue_name}' to exchange '{EXCHANGE_NAME}' with routing key '{routing_key}'...")
                    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=routing_key)
                    log_message(f"Queue binding created")
                    
                    # Publish message
                    log_message(f"Publishing message to exchange '{EXCHANGE_NAME}' with routing key '{routing_key}'...")
                    channel.basic_publish(
                        exchange=EXCHANGE_NAME,
                        routing_key=routing_key,
                        body=message,
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # Make message persistent
                            content_type='application/xml'
                        )
                    )
                    log_message(f"Message published to queue: {queue_name}")
                    success_count += 1
                    
                except Exception as queue_error:
                    log_message(f"Error publishing to queue '{queue_name}': {queue_error}")
            
            log_message("Closing RabbitMQ connection...")
            connection.close()
            log_message(f"RabbitMQ connection closed. Successfully sent create message to {success_count} of {len(TARGET_QUEUES)} queues.")
            return success_count > 0
            
        except pika.exceptions.AMQPConnectionError as e:
            error_msg = f"RabbitMQ connection error: {e}"
            log_message(error_msg)
            return False
        except Exception as e:
            error_msg = f"Failed to publish user create message: {e}"
            log_message(error_msg)
            return False
    
    @api.model
    def create(self, vals):
        """Override the create method to send user data to RabbitMQ."""
        log_message("Creating a new partner...")
        
        # Create the partner
        partner = super(ResPartner, self).create(vals)
        
        # Add partner ID to the recently created set
        # Import the set from the update module
        from odoo.addons.user_update.models.publisher_user_update import ResPartner as UpdateResPartner
        UpdateResPartner._recently_created_partners.add(partner.id)
        
        log_message(f"Added partner ID {partner.id} to recently created set")
        
        # Schedule cleanup of the recently created set after 5 seconds
        self.env.cr.execute("""
            SELECT pg_sleep(5);
            SELECT 1;
        """)
        
        def cleanup_partner_id():
            try:
                UpdateResPartner._recently_created_partners.discard(partner.id)
                log_message(f"Removed partner ID {partner.id} from recently created set")
            except Exception as e:
                log_message(f"Error cleaning up partner ID: {e}")
        
        # Use a thread for cleanup (this ensures we don't block the main thread)
        import threading
        threading.Timer(5, cleanup_partner_id).start()
        
        try:
            # Prepare partner data
            partner_data = {
                'ActionType': 'CREATE',
                'UserID': str(partner.id),
                'TimeOfAction': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                'FirstName': partner.name.split(' ')[0] if partner.name else '',
                'LastName': ' '.join(partner.name.split(' ')[1:]) if partner.name and ' ' in partner.name else '',
                'PhoneNumber': partner.phone or '',
                'EmailAddress': partner.email or '',
            }
            
            # Add business data if applicable
            if partner.is_company or partner.parent_id:
                business_name = partner.name if partner.is_company else partner.parent_id.name
                business_data = {
                    'BusinessName': business_name or '',
                    'BusinessEmail': partner.email or '',
                    'RealAddress': f"{partner.street or ''}, {partner.city or ''}, {partner.zip or ''}" if any([partner.street, partner.city, partner.zip]) else '',
                    'BTWNumber': partner.vat or vals.get('vat', ''),
                    'FacturationAddress': f"{partner.street2 or ''}, {partner.city or ''}, {partner.zip or ''}" if any([partner.street2, partner.city, partner.zip]) else '',
                }
                
                # Only add Business section if there's actual data
                if any(business_data.values()):
                    partner_data['Business'] = business_data

            log_message(f"Partner data prepared: {partner_data}")
            
            # Send the RabbitMQ message
            self.publish_user_create(partner_data)
            
        except Exception as e:
            log_message(f"Error preparing or sending user create message: {e}")
        
        return partner