import pika
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from odoo import models, fields, api
import os
from lxml import etree

_logger = logging.getLogger(__name__)

def log_message(message):
    print(f"[ORDER_MODULE] {message}")
    _logger.info(message)

print("[ORDER_MODULE] Starting Order RabbitMQ Publisher...")

# XSD Schema for Order Messages
ORDER_MESSAGE_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="Order">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="ActionType" type="xs:string"/>
                <xs:element name="OrderID" type="xs:string"/>
                <xs:element name="Date" type="xs:dateTime"/>
                <xs:element name="User" type="xs:string"/>
                <xs:element name="Customer" type="xs:string" minOccurs="0"/>
                <xs:element name="TotalAmount" type="xs:decimal"/>
                <xs:element name="Products" minOccurs="0">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="Product" maxOccurs="unbounded">
                                <xs:complexType>
                                    <xs:sequence>
                                        <xs:element name="ProductName" type="xs:string"/>
                                        <xs:element name="Quantity" type="xs:decimal"/>
                                        <xs:element name="UnitPrice" type="xs:decimal"/>
                                        <xs:element name="TotalPrice" type="xs:decimal"/>
                                    </xs:sequence>
                                </xs:complexType>
                            </xs:element>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
                <xs:element name="Payments" minOccurs="0">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="Payment" maxOccurs="unbounded">
                                <xs:complexType>
                                    <xs:sequence>
                                        <xs:element name="PaymentMethod" type="xs:string"/>
                                        <xs:element name="Amount" type="xs:decimal"/>
                                    </xs:sequence>
                                </xs:complexType>
                            </xs:element>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
                <xs:element name="Taxes" type="xs:decimal" minOccurs="0"/>
                <xs:element name="TotalPaid" type="xs:decimal" minOccurs="0"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>'''

class OrderRabbitMQPublisher(models.AbstractModel):
    _name = 'order.rabbitmq.publisher'
    _description = 'RabbitMQ Publisher for Order Events'

    def _get_rabbitmq_connection_params(self):
        credentials = pika.PlainCredentials(
            os.environ.get('RABBITMQ_USER'),
            os.environ.get('RABBITMQ_PASSWORD')
        )
        return pika.ConnectionParameters(
            host=os.environ.get('RABBITMQ_HOST', 'rabbitmq'),
            port=int(os.environ.get('RABBITMQ_PORT', 5672)),
            credentials=credentials
        )

    def validate_xml_against_xsd(self, xml_string):
        try:
            xml_doc = etree.fromstring(xml_string.encode('utf-8'))
            xsd_doc = etree.fromstring(ORDER_MESSAGE_XSD.encode('utf-8'))
            schema = etree.XMLSchema(xsd_doc)
            return schema.validate(xml_doc)
        except Exception as e:
            log_message(f"XML validation error: {e}")
            return False

    def _publish_message(self, message, queue_name):
        try:
            connection = pika.BlockingConnection(self._get_rabbitmq_connection_params())
            channel = connection.channel()

            routing_key = queue_name
            channel.exchange_declare(exchange='billing', exchange_type='topic', durable=True)
            channel.queue_declare(queue=queue_name, durable=True)
            channel.queue_bind(exchange='billing', queue=queue_name, routing_key=routing_key)

            channel.basic_publish(
                exchange='billing',
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/xml'
                )
            )
            log_message(f"Published order to {queue_name}")
            connection.close()
        except Exception as e:
            log_message(f"Error publishing order message: {e}")

    def create_order_message(self, order):
        root = ET.Element("Order")
        ET.SubElement(root, "ActionType").text = "create"
        ET.SubElement(root, "OrderID").text = str(order.id)
        ET.SubElement(root, "Date").text = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        ET.SubElement(root, "User").text = order.user_id.name or ""
        ET.SubElement(root, "Customer").text = order.partner_id.name if order.partner_id else ""
        ET.SubElement(root, "TotalAmount").text = f"{order.amount_total:.2f}"

        products = ET.SubElement(root, "Products")
        for line in order.lines:
            product = ET.SubElement(products, "Product")
            ET.SubElement(product, "ProductName").text = line.product_id.name
            ET.SubElement(product, "Quantity").text = f"{line.qty:.2f}"
            ET.SubElement(product, "UnitPrice").text = f"{line.price_unit:.2f}"
            ET.SubElement(product, "TotalPrice").text = f"{line.price_subtotal:.2f}"

        xml_str = ET.tostring(root, encoding='unicode')
        if not self.validate_xml_against_xsd(xml_str):
            log_message("Generated order XML failed XSD validation")
        return xml_str

    def publish_orders_for_session(self, session):
        log_message(f"Publishing orders for session: {session.name}")
        for order in session.order_ids.filtered(lambda o: o.state == 'done'):
            log_message(f"[DEBUG] Order {order.id} has state: {order.state}")
            self.publish_order_event(order)


    def publish_order_event(self, order):
        log_message(f"Preparing to publish individual order ID {order.id}")
        message = self.create_order_message(order)
        self._publish_message(message, queue_name="order.created")

class PosOrder(models.Model):
    _inherit = 'pos.order'

    @api.model
    def create(self, vals):
        order = super().create(vals)
        # Do not send to RabbitMQ yet; wait for session close
        log_message(f"Order {order.id} created but not published (will publish on session close).")
        return order

class PosSession(models.Model):
    _inherit = 'pos.session'

    def action_pos_session_open(self):
        log_message(f"POS session '{self.name}' is being opened.")
        return super().action_pos_session_open()

    def action_pos_session_close(self, balancing_account=False, amount_to_balance=0.0, bank_payment_method_diffs=None):
        result = super().action_pos_session_close(
            balancing_account,
            amount_to_balance,
            bank_payment_method_diffs
        )
        for session in self:
            log_message(f"POS session '{session.name}' is fully closed. Now publishing orders.")
            self.env['order.rabbitmq.publisher'].publish_orders_for_session(session)
        return result


