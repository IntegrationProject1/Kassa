import pika
import xml.etree.ElementTree as ET
import logging
import os
from odoo import models, api

_logger = logging.getLogger(__name__)

# Configuration with environment variable fallbacks
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
QUEUE_NAME = os.environ.get('POS_QUEUE', 'orders')
SERVICE_NAME = os.environ.get('SERVICE_NAME', 'Odoo_POS')

class PosOrder(models.Model):
    _inherit = 'pos.order'

    @api.model
    def create(self, vals):
        order = super(PosOrder, self).create(vals)
        try:
            xml_message = self.generate_xml_message(order)
            self.send_to_rabbitmq(xml_message)
        except Exception as e:
            _logger.error(f"Failed to send order to RabbitMQ: {e}")
        return order

    def generate_xml_message(self, order):
        """Generates an XML message for the POS order."""
        root = ET.Element("Order")
        ET.SubElement(root, "OrderID").text = str(order.id)
        
        # Handle potentially missing partner gracefully
        customer_name = order.partner_id.name if order.partner_id else ""
        ET.SubElement(root, "Customer").text = customer_name
        
        ET.SubElement(root, "TotalAmount").text = str(order.amount_total)
        return ET.tostring(root, encoding='unicode')

    def send_to_rabbitmq(self, message):
        """Sends a message to RabbitMQ with proper error handling."""
        connection = None
        try:
            # Use environment variable for host
            credentials = pika.PlainCredentials('guest', 'guest')
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=2
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Ensure queue exists and is durable (messages survive restart)
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            
            # Send the message with persistence enabled
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/xml'
                )
            )
            
            _logger.info(f"Order message sent to RabbitMQ queue: {QUEUE_NAME}")
            
        except pika.exceptions.AMQPConnectionError as e:
            _logger.error(f"RabbitMQ connection error: {e}")
            raise
        except Exception as e:
            _logger.error(f"Error sending message to RabbitMQ: {e}")
            raise
        finally:
            # Ensure connection is closed even if an error occurs
            if connection and connection.is_open:
                connection.close()
