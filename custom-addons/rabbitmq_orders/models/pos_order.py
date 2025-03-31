import pika
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from odoo import models, api
import os
from lxml import etree

_logger = logging.getLogger(__name__)

def log_message(message):
    print(f"[ORDER_MODULE] {message}")
    _logger.info(message)

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

    def create_order_message(self, order, action_type):
        root = ET.Element("Order")
        ET.SubElement(root, "ActionType").text = action_type
        ET.SubElement(root, "OrderID").text = str(order.id)
        ET.SubElement(root, "Date").text = order.date_order.strftime("%Y-%m-%dT%H:%M:%SZ")
        ET.SubElement(root, "User").text = order.user_id.name or ""
        ET.SubElement(root, "Customer").text = order.partner_id.name if order.partner_id else ""
        
        total_amount = abs(order.amount_total) if action_type == "refunded" else order.amount_total
        ET.SubElement(root, "TotalAmount").text = f"{total_amount:.2f}"

        if action_type in ["create", "refunded"]:
            products = ET.SubElement(root, "Products")
            for line in order.lines:
                product = ET.SubElement(products, "Product")
                ET.SubElement(product, "ProductName").text = line.product_id.name
                qty = abs(line.qty) if action_type == "refunded" else line.qty
                ET.SubElement(product, "Quantity").text = f"{qty:.2f}"
                price = abs(line.price_unit) if action_type == "refunded" else line.price_unit
                ET.SubElement(product, "UnitPrice").text = f"{price:.2f}"
                subtotal = abs(line.price_subtotal) if action_type == "refunded" else line.price_subtotal
                ET.SubElement(product, "TotalPrice").text = f"{subtotal:.2f}"

            payments = ET.SubElement(root, "Payments")
            for payment in order.payment_ids:
                pmt = ET.SubElement(payments, "Payment")
                ET.SubElement(pmt, "PaymentMethod").text = payment.payment_method_id.name
                amount = abs(payment.amount) if action_type == "refunded" else payment.amount
                ET.SubElement(pmt, "Amount").text = f"{amount:.2f}"

            taxes = abs(order.amount_tax) if action_type == "refunded" else order.amount_tax
            ET.SubElement(root, "Taxes").text = f"{taxes:.2f}"
            total_paid = abs(order.amount_paid) if action_type == "refunded" else order.amount_paid
            ET.SubElement(root, "TotalPaid").text = f"{total_paid:.2f}"

        xml_str = ET.tostring(root, encoding='unicode')
        if not self.validate_xml_against_xsd(xml_str):
            log_message("Generated order XML failed XSD validation")
        return xml_str

    def publish_order_event(self, order, action_type):
        try:
            if order.amount_total < 0 and action_type == "create":
                action_type = "refunded"
                log_message(f"Auto-corrected action_type to 'refunded' for order {order.id}")

            message = self.create_order_message(order, action_type)
            queue_name = "order.created" if action_type == "create" else "order.refunded"
            routing_key = queue_name

            connection = pika.BlockingConnection(self._get_rabbitmq_connection_params())
            channel = connection.channel()

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
            log_message(f"Published order {order.id} to {queue_name}")
            connection.close()
        except Exception as e:
            log_message(f"Error publishing order event: {e}")

class PosOrder(models.Model):
    _inherit = 'pos.order'

    @api.model
    def create(self, vals):
        order = super().create(vals)
        self.env['order.rabbitmq.publisher'].publish_order_event(order, 'create')
        return order

    def action_pos_order_refund(self):
        result = super().action_pos_order_refund()
        for order in self:
            self.env['order.rabbitmq.publisher'].publish_order_event(order, 'refunded')
        return result