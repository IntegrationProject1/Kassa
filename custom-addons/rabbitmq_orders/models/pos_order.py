from odoo import models, fields, api
from datetime import datetime
import xml.etree.ElementTree as ET
import pika
import os
import logging
from lxml import etree

_logger = logging.getLogger(__name__)

def log_message(message):
    print(f"[ORDER_MODULE] {message}")
    _logger.info(message)

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

class Event(models.Model):
    _name = 'event.event'
    _description = 'External Event'
    _order = 'start_datetime desc'

    uuid = fields.Char(required=True, string="UUID", index=True)
    name = fields.Char(required=True)
    description = fields.Text()
    start_datetime = fields.Char(required=True)
    end_datetime = fields.Char(required=True)
    location = fields.Char()
    organisator = fields.Char()
    capacity = fields.Integer()
    event_type = fields.Char()

    registered_user_ids = fields.Many2many(
        'res.partner',
        'event_event_res_partner_rel',
        'event_event_id',
        'res_partner_id',
        string='Registered Users',
        help='Users registered for this event (linked by external_id)',
    )


class EventOrder(models.Model):
    _name = 'event.order'
    _description = 'Order linked to an Event and User'

    event_id = fields.Many2one('event.event', required=True)
    partner_id = fields.Many2one('res.partner', required=True)
    order_date = fields.Datetime(string='Order Date', default=fields.Datetime.now)
    order_line_ids = fields.One2many('event.order.product', 'event_order_id', string='Order Lines')


class EventOrderProduct(models.Model):
    _name = 'event.order.product'
    _description = 'Product in an Event Order'

    event_order_id = fields.Many2one('event.order', required=True, ondelete='cascade')
    product_nr = fields.Char(required=True)
    quantity = fields.Float(required=True)
    unit_price = fields.Float(required=True)


class PosOrder(models.Model):
    _inherit = 'pos.order'

    @api.model
    def create(self, vals):
        order = super().create(vals)
        log_message(f"Order {order.id} created, checking for event linkage.")
        self.env['order.rabbitmq.publisher']._handle_order(order)
        return order


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

    def _handle_order(self, order):
        now = datetime.utcnow()
        partner = order.partner_id

        event = self.env['event.event'].search([
            ('start_datetime', '<=', now.strftime('%Y-%m-%d %H:%M:%S')),
            ('end_datetime', '>=', now.strftime('%Y-%m-%d %H:%M:%S')),
            ('registered_user_ids', 'in', partner.id)
        ], limit=1)

        if event:
            log_message(f"Found active event '{event.name}' for user '{partner.name}'")
            self._store_event_order(order, event)
        else:
            log_message(f"No active event found for user '{partner.name}', sending to queue")
            self._publish_order_to_queue(order)

    def _store_event_order(self, order, event):
        event_order = self.env['event.order'].create({
            'event_id': event.id,
            'partner_id': order.partner_id.id,
            'order_date': fields.Datetime.now(),
        })
        for line in order.lines:
            self.env['event.order.product'].create({
                'event_order_id': event_order.id,
                'product_nr': str(line.product_id.id),
                'quantity': line.qty,
                'unit_price': line.price_unit,
            })
        log_message(f"Order {order.id} stored in event '{event.name}'")

    def _publish_order_to_queue(self, order):
        partner = order.partner_id
        uuid_value = partner.external_id

        if not uuid_value:
            log_message(f"Missing external_id for partner {partner.name}, skipping publish")
            return

        root = ET.Element("Order")
        ET.SubElement(root, "Date").text = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        ET.SubElement(root, "UUID").text = uuid_value

        products = ET.SubElement(root, "Products")
        for line in order.lines:
            product = ET.SubElement(products, "Product")
            ET.SubElement(product, "ProductNR").text = str(line.product_id.id)
            ET.SubElement(product, "Quantity").text = f"{line.qty:.2f}"
            ET.SubElement(product, "UnitPrice").text = f"{line.price_unit:.2f}"

        xml_str = ET.tostring(root, encoding='unicode')

        if not self.validate_xml_against_xsd(xml_str):
            log_message("Generated order XML failed XSD validation")
            return

        self._publish_message(xml_str, queue_name="order.created")
