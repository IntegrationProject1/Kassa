import pika
import xml.etree.ElementTree as ET
from odoo import models, api
import logging
import os

_logger = logging.getLogger(__name__)

# Load RabbitMQ configuration from environment variables
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')

QUEUE_CREATED = "order.created"
QUEUE_REFUNDED = "order.refunded"  # Dit is de queue voor refunds

class PosOrder(models.Model):
    _inherit = 'pos.order'

    @api.model
    def create(self, vals):
        """Aangemaakte POS-order versturen naar RabbitMQ (order.created queue)."""
        order = super(PosOrder, self).create(vals)
        _logger.info(f"Order {order.id} aangemaakt in Odoo. Versturen naar {QUEUE_CREATED}...")
        
        xml_message = self.generate_xml_message(order, action_type="create")
        self.send_to_rabbitmq(xml_message, QUEUE_CREATED)
        return order

    def action_pos_order_refund(self):
        """Refunds the order and sends a message to RabbitMQ (order.refunded queue)."""
        result = super(PosOrder, self).action_pos_order_refund()
        
        # Verzend refund bericht voor elke order
        for order in self:
            _logger.info(f"Order {order.id} wordt refunded. Versturen naar {QUEUE_REFUNDED}...")
            
            # Zoek de refund order (met negatieve bedragen)
            refund_order = self.search([('id', '=', order.id)], limit=1)
            
            # Genereer XML met action_type="refunded"
            xml_message = self.generate_xml_message(refund_order, action_type="refunded")
            
            # Verstuur naar de specifieke refund queue
            self.send_to_rabbitmq(xml_message, QUEUE_REFUNDED)
        
        return result

    def generate_xml_message(self, order, action_type):
        """Genereert een XML bericht met ordergegevens, inclusief ActieType."""
        _logger.info(f"Genereer XML bericht voor order {order.id} met actie {action_type}...")
        root = ET.Element("Order")

        # Controleer of dit een refund order is (negatieve bedragen)
        if order.amount_total < 0 and action_type == "create":
            action_type = "refunded"
            _logger.warning(f"Order {order.id} heeft negatieve bedragen maar action_type was 'create'. Gewijzigd naar 'refunded'")

        # ActieType (create/refunded)
        ET.SubElement(root, "ActionType").text = action_type

        # Gebruik absolute waarden voor refunds
        total_amount = abs(order.amount_total) if action_type == "refunded" else order.amount_total
        
        # Orderinformatie
        ET.SubElement(root, "OrderID").text = str(order.id)
        ET.SubElement(root, "Date").text = order.date_order.strftime("%m/%d/%Y %H:%M:%S")
        ET.SubElement(root, "User").text = order.user_id.name
        ET.SubElement(root, "Customer").text = order.partner_id.name if order.partner_id else ""
        ET.SubElement(root, "TotalAmount").text = str(total_amount)

        if action_type in ["create", "refunded"]:
            # Producten
            products_element = ET.SubElement(root, "Products")
            for line in order.lines:
                product = ET.SubElement(products_element, "Product")
                ET.SubElement(product, "ProductName").text = line.product_id.name
                ET.SubElement(product, "Quantity").text = str(abs(line.qty) if action_type == "refunded" else line.qty)
                ET.SubElement(product, "UnitPrice").text = str(abs(line.price_unit) if action_type == "refunded" else line.price_unit)
                ET.SubElement(product, "TotalPrice").text = str(abs(line.price_subtotal) if action_type == "refunded" else line.price_subtotal)

            # Betalingen
            payments_element = ET.SubElement(root, "Payments")
            for payment in order.payment_ids:
                payment_method = ET.SubElement(payments_element, "Payment")
                ET.SubElement(payment_method, "PaymentMethod").text = payment.payment_method_id.name
                ET.SubElement(payment_method, "Amount").text = str(abs(payment.amount) if action_type == "refunded" else str(payment.amount))

            # Belasting en totaal
            ET.SubElement(root, "Taxes").text = str(abs(order.amount_tax) if action_type == "refunded" else order.amount_tax)
            ET.SubElement(root, "TotalPaid").text = str(abs(order.amount_paid) if action_type == "refunded" else order.amount_paid)

        return ET.tostring(root, encoding="unicode")

    def send_to_rabbitmq(self, message, queue_name):
        """Verstuurt een bericht naar de specifieke RabbitMQ queue."""
        try:
            _logger.info(f"Verbinding maken met RabbitMQ op {RABBITMQ_HOST}:{RABBITMQ_PORT} voor queue {queue_name}...")
            
            credentials = pika.PlainCredentials(RABBITMQ_USER, os.environ.get('RABBITMQ_PASSWORD'))
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=RABBITMQ_HOST, 
                port=RABBITMQ_PORT, 
                credentials=credentials))
            channel = connection.channel()

            # Declareer de queue met durable=True voor persistentie
            _logger.info(f"Declareren van queue: {queue_name}...")
            channel.queue_declare(queue=queue_name, durable=True)
            channel.exchange_declare(exchange="billing", exchange_type="topic", durable=True)
            channel.queue_bind(exchange="billing", queue=queue_name, routing_key=queue_name)

            # Publiceer het bericht
            _logger.info(f"Verzenden naar queue {queue_name}...")
            channel.basic_publish(
                exchange='billing',
                routing_key=queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Maak bericht persistent
                    content_type="application/xml"
                )
            )

            _logger.info(f"Bericht succesvol verzonden naar {queue_name}")
            connection.close()
        except pika.exceptions.AMQPConnectionError as e:
            _logger.error(f"RabbitMQ verbindingsfout voor queue {queue_name}: {e}")
        except Exception as e:
            _logger.error(f"Fout bij verzenden naar queue {queue_name}: {e}")