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
                <xs:element name="UUID" type="xs:int"/>
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
    print(f"[CUSTOMER_CREATE_MODULE] {message}")
    _logger.info(message)

log_message("RabbitMQ Customer Create Publisher loaded")

# Create a shared prevention registry to avoid circular imports
class PreventionRegistry:
    """Static registry to track recently created partner IDs"""
    recently_created_partners = set()

# Create global instance
prevention_registry = PreventionRegistry()

class ResPartner(models.Model):
    _inherit = 'res.partner'
    
    # Track recently created partner IDs to prevent duplicate notifications
    _recently_created_ids = set()
    
    external_id = fields.Char(string="External ID", 
                            help="External identifier for integration with other systems",
                            index=True)
    
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
    
    def create_customer_create_message(self, partner_data):
        """Create XML message for customer creation"""
        root = ET.Element("UserMessage")  # Keep as UserMessage per XSD schema
        
        # Add regular elements
        for key, value in partner_data.items():
            if key == 'Business':
                continue  # Handle business separately
            if value is not None:
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
    
    def publish_customer_create(self, partner_data):
        """Publish customer create message to other service queues"""
        try:
            customer_id = partner_data.get('UUID')  # Changed from UserID to UUID
            log_message(f"Publishing customer create message for customer_id: {customer_id}")
            
            # Create the message
            message = self.create_customer_create_message(partner_data)
            
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
            log_message(f"Failed to publish customer create message: {e}")
            return False
    
    @api.model
    def create(self, vals):
        """Override the create method to send customer data to RabbitMQ."""
        log_message("Creating a new partner...")
        
        # Generate an auto-incremented external_id if this is a customer
        if vals.get('customer_rank', 0) > 0 and not vals.get('external_id'):
            # Find the highest existing external_id that is numeric
            last_id = 0
            partners_with_ext_id = self.search([('external_id', '!=', False)])
            for partner in partners_with_ext_id:
                try:
                    ext_id_num = int(partner.external_id)
                    if ext_id_num > last_id:
                        last_id = ext_id_num
                except (ValueError, TypeError):
                    pass  # Skip non-numeric external_ids
            
            # Set the next external_id
            vals['external_id'] = str(last_id + 1)
            log_message(f"Generated new external_id: {vals['external_id']}")
        
        # Set a context flag for the child write operations
        ctx = dict(self.env.context, creating_new_partner=True)
        
        # Create the partner with our special context
        partner = super(ResPartner, self.with_context(ctx)).create(vals)
        
        # Check if we should skip publishing (when created from RabbitMQ)
        if self.env.context.get('skip_rabbitmq_publish'):
            log_message(f"Skipping RabbitMQ publish for partner {partner.id} (created from RabbitMQ)")
            return partner
        
        # CRITICAL: Only send messages for actual customers
        if not partner.customer_rank > 0:
            log_message(f"Partner {partner.id} is not a customer (customer_rank={partner.customer_rank}), skipping message")
            return partner
        
        # Add to both prevention registries
        self._recently_created_ids.add(partner.id)
        prevention_registry.recently_created_partners.add(partner.id)
        log_message(f"Added customer ID {partner.id} to prevention registry")
        
        # Use threading instead of pg_sleep which can block database
        import threading
        def cleanup_ids():
            try:
                # Clean up both sets
                if partner.id in self._recently_created_ids:
                    self._recently_created_ids.discard(partner.id)
                prevention_registry.recently_created_partners.discard(partner.id)
                log_message(f"Removed customer ID {partner.id} from prevention registry")
            except Exception as e:
                log_message(f"Error in cleanup: {e}")
        
        # Schedule cleanup after 10 seconds
        threading.Timer(10.0, cleanup_ids).start()
        
        try:
            # Prepare customer data
            partner_data = {
                'ActionType': 'CREATE',
                'UUID': int(partner.external_id or partner.id),  # Changed UserID to UUID, ensure it's an integer
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

            # Send the RabbitMQ message
            self.publish_customer_create(partner_data)
            
        except Exception as e:
            log_message(f"Error preparing or sending customer create message: {e}")
        
        return partner
    
    def write(self, vals):
        """Override the write method to send customer data updates to RabbitMQ"""
        # If customer_rank is being set to > 0 and there's no external_id, generate one
        if vals.get('customer_rank', 0) > 0:
            for record in self.filtered(lambda r: not r.external_id):
                # Find the highest existing external_id that is numeric
                last_id = 0
                partners_with_ext_id = self.search([('external_id', '!=', False)])
                for partner in partners_with_ext_id:
                    try:
                        ext_id_num = int(partner.external_id)
                        if ext_id_num > last_id:
                            last_id = ext_id_num
                    except (ValueError, TypeError):
                        pass  # Skip non-numeric external_ids
                
                # Set the next external_id
                if 'external_id' not in vals:
                    vals['external_id'] = str(last_id + 1)
                    log_message(f"Generated new external_id on update: {vals['external_id']}")

        result = super(ResPartner, self).write(vals)
        # Rest of your existing write method code...
        return result