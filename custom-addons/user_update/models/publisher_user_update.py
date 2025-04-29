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

# Exchange and target queues definition
EXCHANGE_NAME = 'user'
TARGET_QUEUES = [
    {'queue': 'crm_user_update', 'routing_key': 'crm.user.update'},
    {'queue': 'facturatie_user_update', 'routing_key': 'facturatie.user.update'},
    {'queue': 'frontend_user_update', 'routing_key': 'frontend.user.update'}
]

# XSD Schema for validation
USER_UPDATE_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
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

def log_message(message):
    """Standard logging function"""
    print(f"[CUSTOMER_UPDATE_MODULE] {message}")
    _logger.info(message)

log_message("RabbitMQ Customer Update Publisher loaded")

# Import prevention registry to avoid circular imports
try:
    from odoo.addons.user_create.models.publisher_user_create import prevention_registry
    log_message("Successfully imported prevention registry")
except ImportError:
    log_message("Could not import prevention registry, creating local instance")
    class PreventionRegistry:
        recently_created_partners = set()
    prevention_registry = PreventionRegistry()

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
    
    def create_customer_update_message(self, partner_data):
        """Create XML message for customer update"""
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
        is_valid = self.validate_xml_against_xsd(xml_string, USER_UPDATE_XSD)
        if not is_valid:
            log_message("Generated XML does not conform to XSD schema")
            
        return xml_string
    
    def publish_customer_update(self, partner_data):
        """Publish customer update message to other service queues"""
        try:
            customer_id = partner_data.get('UUID')  # Changed from UserID to UUID
            log_message(f"Publishing customer update message for customer_id: {customer_id}")
            
            # Create the message
            message = self.create_customer_update_message(partner_data)
            
            # Connect to RabbitMQ
            connection = pika.BlockingConnection(self._get_rabbitmq_connection_params())
            channel = connection.channel()
            
            # Ensure the exchange exists
            channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)
            
            # Publish to each target queue
            success_count = 0
            for target in TARGET_QUEUES:
                queue_name = target['queue']
                routing_key = target['routing_key']
                
                try:
                    # Ensure the queue exists
                    channel.queue_declare(queue=queue_name, durable=True)
                    
                    # Bind queue to exchange
                    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=routing_key)
                    
                    # Publish message
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
            
            connection.close()
            return success_count > 0
            
        except Exception as e:
            log_message(f"Failed to publish customer update message: {e}")
            return False
    
    def write(self, vals):
        """Override the write method to send a RabbitMQ message on update."""
        log_message(f"Updating partner(s): {self.ids}")
        
        # Skip if we're in the process of creating a partner
        if self.env.context.get('creating_new_partner'):
            log_message("Skipping update during partner creation")
            return super(ResPartner, self).write(vals)
        
        # Skip if explicitly requested in context
        if self.env.context.get('skip_rabbitmq_message'):
            log_message("Skipping RabbitMQ message due to context flag")
            return super(ResPartner, self).write(vals)
        
        # If this is a customer and we're adding customer_rank but there's no external_id, generate one
        if vals.get('customer_rank', 0) > 0:
            for record in self.filtered(lambda r: not r.external_id):
                # Generate timestamp with microsecond precision for external_id
                timestamp_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                
                # Set the timestamp as external_id
                if 'external_id' not in vals:
                    vals['external_id'] = timestamp_id
                    log_message(f"Generated new timestamp-based external_id on update: {vals['external_id']}")
        
        # Check if any of these partners were recently created
        partners_to_skip = []
        partners_to_process = []
        
        for partner in self:
            # Skip if not a customer
            if not partner.customer_rank > 0:
                partners_to_skip.append(partner.id)
                continue
                
            # Check both prevention registries
            if partner.id in prevention_registry.recently_created_partners:
                log_message(f"Partner {partner.id} was recently created (registry), skipping update")
                partners_to_skip.append(partner.id)
                continue
                
            # Process this partner
            partners_to_process.append(partner.id)
        
        # Call the original write method
        result = super(ResPartner, self).write(vals)
        
        # Only process partners that weren't recently created and are customers
        if partners_to_process:
            partners_to_update = self.env['res.partner'].browse(partners_to_process)
            
            for partner in partners_to_update:
                # Use existing external_id instead of generating a new timestamp
                partner_uuid = partner.external_id
                
                # If external_id is not set, fallback to a timestamp
                if not partner_uuid:
                    partner_uuid = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    log_message(f"Warning: No external_id found for partner {partner.id}, using timestamp instead")
                
                # Basic customer data
                partner_data = {
                    'ActionType': 'UPDATE',
                    'UUID': partner_uuid,  # Using existing external_id
                    'TimeOfAction': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # Keeping microsecond precision for action time
                    'EncryptedPassword': 'odooadmin',  # Standard password as requested
                    'FirstName': partner.name.split(' ')[0] if partner.name else '',
                    'LastName': ' '.join(partner.name.split(' ')[1:]) if partner.name and ' ' in partner.name else '',
                    'PhoneNumber': partner.phone or '',
                    'EmailAddress': partner.email or '',
                }
                
                # Add business data
                if partner.is_company or partner.parent_id or partner.company_name:
                    # Determine business name
                    business_name = ''
                    if partner.is_company:
                        business_name = partner.name
                    elif partner.parent_id:
                        business_name = partner.parent_id.name
                    elif partner.company_name:
                        business_name = partner.company_name
                        
                    business_data = {
                        'BusinessName': business_name or '',
                        'BusinessEmail': partner.email or '',
                        'RealAddress': f"{partner.street or ''}, {partner.city or ''}, {partner.zip or ''}" if any([partner.street, partner.city, partner.zip]) else '',
                        'BTWNumber': partner.vat or '',
                        'FacturationAddress': f"{partner.street2 or ''}, {partner.city or ''}, {partner.zip or ''}" if any([partner.street2, partner.city, partner.zip]) else '',
                    }
                    
                    # Only add Business section if there's actual data
                    if any(business_data.values()):
                        partner_data['Business'] = business_data

                # Send the RabbitMQ message
                self.publish_customer_update(partner_data)

        return result