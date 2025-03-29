import pika
import xml.etree.ElementTree as ET
from odoo import models, api
import logging
import os

_logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')



QUEUE_CREATED = "order.created"
QUEUE_REFUNDED = "order.refunded"

class PosOrder(models.Model):
    _inherit = 'pos.order'

    def _get_rabbitmq_connection_params(self):
        """Get RabbitMQ connection parameters from environment variables"""
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        return pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )

    def create(self, vals):
        """Aangemaakte POS-order versturen naar RabbitMQ."""
        order = super(PosOrder, self).create(vals)
        _logger.info(f"Order {order.id} aangemaakt in Odoo. Versturen naar RabbitMQ...")

        xml_message = self.generate_xml_message(order, action_type="create")
        self.send_to_rabbitmq(xml_message, QUEUE_CREATED)
        return order

    def action_pos_order_refund(self):
        """Refundt de order en stuurt een bericht naar RabbitMQ."""
        for order in self:
            _logger.info(f"Order {order.id} wordt refunded. Versturen naar RabbitMQ...")
            xml_message = self.generate_xml_message(order, action_type="refunded")
            self.send_to_rabbitmq(xml_message, QUEUE_REFUNDED)
        return super(PosOrder, self).action_pos_order_refund()

    def generate_xml_message(self, order, action_type):
        """Genereert een XML bericht met ordergegevens, inclusief ActieType."""
        _logger.info(f"Genereer XML bericht voor order {order.id} met actie {action_type}...")
        root = ET.Element("Order")
        
        # ActieType (create/refunded)
        ET.SubElement(root, "ActionType").text = action_type
        
        # Orderinformatie
        ET.SubElement(root, "OrderID").text = str(order.id)
        ET.SubElement(root, "Date").text = order.date_order.strftime("%m/%d/%Y %H:%M:%S")
        ET.SubElement(root, "User").text = order.user_id.name

        if action_type in ["create", "refunded"]:
            products_element = ET.SubElement(root, "Products")
            for line in order.lines:
                product = ET.SubElement(products_element, "Product")
                ET.SubElement(product, "ProductName").text = line.product_id.name
                ET.SubElement(product, "Quantity").text = str(line.qty)
                ET.SubElement(product, "UnitPrice").text = str(line.price_unit)
                ET.SubElement(product, "TotalPrice").text = str(line.price_subtotal)

            # Betalingen
            payments_element = ET.SubElement(root, "Payments")
            for payment in order.payment_ids:
                payment_method = ET.SubElement(payments_element, "Payment")
                ET.SubElement(payment_method, "PaymentMethod").text = payment.payment_method_id.name
                ET.SubElement(payment_method, "Amount").text = str(payment.amount)

            # Extra informatie zoals belasting en totaal
            ET.SubElement(root, "TotalAmount").text = str(order.amount_total)
            ET.SubElement(root, "Taxes").text = str(order.amount_tax)
            ET.SubElement(root, "TotalPaid").text = str(order.amount_paid)

        return ET.tostring(root, encoding="unicode")

    def send_to_rabbitmq(self, message, queue_name):
        """Verstuurt een bericht naar RabbitMQ en probeert opnieuw bij een fout."""
        try:
            _logger.info(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
            connection = pika.BlockingConnection(self._get_rabbitmq_connection_params())
            channel = connection.channel()

            _logger.info(f"Declaring RabbitMQ queue: {queue_name}...")
            channel.queue_declare(queue=queue_name, durable=True)
            channel.exchange_declare(exchange="billing", exchange_type="topic", durable=True)
            channel.queue_bind(exchange="billing", queue=queue_name, routing_key=queue_name)

            _logger.info(f"Publishing message to queue {queue_name}...")
            channel.basic_publish(
                exchange='billing',
                routing_key=queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
            )

            _logger.info(f" [x] Order {message} sent to RabbitMQ.")
            connection.close()
        except pika.exceptions.AMQPConnectionError as e:
            _logger.error(f" [ERROR] RabbitMQ connection error: {e}")
        except Exception as e:
            _logger.error(f" [ERROR] Failed to send message to RabbitMQ: {e}")

