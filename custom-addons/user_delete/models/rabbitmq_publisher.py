import pika
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from odoo import models, fields, api
import os
from lxml import etree

_logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT')) 
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')

# XSD Schema for validation
USER_MESSAGE_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
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
</xs:schema>'''

def log_message(message):
    print(f"[CUSTOMER_DELETE_MODULE] {message}")
    _logger.info(message)

log_message("RabbitMQ Publisher loaded")

class RabbitMQPublisher(models.AbstractModel):
    _name = 'customer.delete.rabbitmq.publisher'
    _description = 'RabbitMQ Publisher for Customer Deletion'

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
    
    def create_customer_delete_message(self, customer_id, external_id=None):
        """Create XML message for customer deletion"""
        # Create the root element
        root = ET.Element("UserMessage")
        
        # Add child elements
        action_type = ET.SubElement(root, "ActionType")
        action_type.text = "DELETE"
        
        user_id_elem = ET.SubElement(root, "UserID")
        # Use external_id if available, otherwise use customer_id
        user_id_elem.text = str(external_id) if external_id else str(customer_id)
        
        time_of_action = ET.SubElement(root, "TimeOfAction")
        time_of_action.text = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Convert to XML string
        xml_string = ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8')
        
        # Validate against XSD schema
        is_valid = self.validate_xml_against_xsd(xml_string, USER_MESSAGE_XSD)
        if not is_valid:
            log_message("Generated XML does not conform to XSD schema")
            
        return xml_string
    
    def publish_customer_delete(self, customer_id, external_id=None):
        """Publish customer deletion message to all other service queues"""
        try:
            identifier = external_id if external_id else customer_id
            log_message(f"Publishing customer deletion message for customer identifier: {identifier}")
            
            # Update target queues to be customer-focused
            target_queues = [
                {'queue': 'crm_user_delete', 'routing_key': 'crm.user.delete'},
                {'queue': 'facturatie_user_delete', 'routing_key': 'facturatie.user.delete'},
                {'queue': 'frontend_user_delete', 'routing_key': 'frontend.user.delete'}
            ]
            
            # Create the message
            message = self.create_customer_delete_message(customer_id, external_id)
            log_message("Message created successfully")
            
            # Connect to RabbitMQ
            log_message(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
            connection = pika.BlockingConnection(self._get_rabbitmq_connection_params())
            log_message("RabbitMQ connection established")
            
            channel = connection.channel()
            log_message("RabbitMQ channel created")
            
            # Use existing exchange
            exchange_name = 'user'
            success_count = 0
            
            # Publish to each queue
            for target in target_queues:
                queue_name = target['queue']
                routing_key = target['routing_key']
                
                try:
                    # Ensure the queue exists
                    log_message(f"Declaring queue '{queue_name}'...")
                    channel.queue_declare(queue=queue_name, durable=True)
                    
                    # Bind queue to exchange with the correct routing key
                    log_message(f"Binding queue '{queue_name}' to exchange '{exchange_name}' with routing key '{routing_key}'...")
                    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)
                    
                    # Publish message with the correct routing key
                    log_message(f"Publishing message to exchange '{exchange_name}' with routing key '{routing_key}'...")
                    channel.basic_publish(
                        exchange=exchange_name,
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
            log_message(f"RabbitMQ connection closed. Successfully sent message to {success_count} of {len(target_queues)} queues.")
            return success_count > 0
            
        except pika.exceptions.AMQPConnectionError as e:
            error_msg = f"RabbitMQ connection error: {e}"
            log_message(error_msg)
            return False
        except Exception as e:
            error_msg = f"Failed to publish customer deletion message: {e}"
            log_message(error_msg)
            return False
        


class ResPartner(models.Model):
    _inherit = 'res.partner'
    
    def unlink(self):
        """Override unlink to publish customer deletion events"""
        # Store IDs and external IDs of customers to be deleted
        customers_to_delete = []
        
        for partner in self:
            if partner.customer_rank > 0:  # Only process actual customers
                customers_to_delete.append({
                    'id': partner.id,
                    'external_id': partner.external_id
                })
                log_message(f"Will delete customer {partner.id} with external_id: {partner.external_id}")
        
        # Call the original unlink method
        result = super(ResPartner, self).unlink()
        
        # Now publish the deletion messages
        if customers_to_delete:
            publisher = self.env['customer.delete.rabbitmq.publisher']
            for customer in customers_to_delete:
                publisher.publish_customer_delete(customer['id'], customer['external_id'])
                
        return result