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

# XSD Schema for Order Messages (updated)
ORDER_MESSAGE_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="Order">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="Date" type="xs:dateTime"/>
        <xs:element name="UUID" type="xs:dateTime"/>
        <xs:element name="Products">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="Product" maxOccurs="unbounded">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="ProductNR" type="xs:decimal"/>
                    <xs:element name="Quantity" type="xs:decimal"/>
                    <xs:element name="UnitPrice" type="xs:decimal"/>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
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

    def publish_orders_for_session(self, session):
        log_message(f"Publishing orders for session: {session.name}")
        orders_by_customer = {}
        for order in session.order_ids.filtered(lambda o: o.state == 'done'):
            if not order.partner_id:
                continue
            customer_id = order.partner_id.id
            orders_by_customer.setdefault(customer_id, []).append(order)

        for customer_orders in orders_by_customer.values():
            self.publish_consolidated_order(customer_orders)

    def publish_consolidated_order(self, orders):
        if not orders:
            return

        root = ET.Element("Order")

        current_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        ET.SubElement(root, "Date").text = current_timestamp

        partner = orders[0].partner_id
        uuid_value = partner.external_id

        # 🔒 Consistent with customer create: fail hard if UUID is missing
        if not uuid_value:
            error_msg = f"Missing external_id (UUID) for partner '{partner.name}' (ID: {partner.id})"
            log_message(error_msg)
            raise ValueError(error_msg)

        ET.SubElement(root, "UUID").text = uuid_value

        # ➕ Products
        products = ET.SubElement(root, "Products")
        for order in orders:
            for line in order.lines:
                product = ET.SubElement(products, "Product")
                ET.SubElement(product, "ProductNR").text = str(line.product_id.id)
                ET.SubElement(product, "Quantity").text = f"{line.qty:.2f}"
                ET.SubElement(product, "UnitPrice").text = f"{line.price_unit:.2f}"

        xml_str = ET.tostring(root, encoding='unicode')

        if not self.validate_xml_against_xsd(xml_str):
            log_message("Generated consolidated order XML failed XSD validation")
        else:
            self._publish_message(xml_str, queue_name="order.created")


class PosOrder(models.Model):
    _inherit = 'pos.order'

    @api.model
    def create(self, vals):
        order = super().create(vals)
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
