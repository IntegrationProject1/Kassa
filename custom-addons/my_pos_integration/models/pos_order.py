import pika
import xml.etree.ElementTree as ET
from odoo import models, api

class PosOrder(models.Model):
    _inherit = 'pos.order'

    @api.model
    def create(self, vals):
        order = super(PosOrder, self).create(vals)
        xml_message = self.generate_xml_message(order)
        self.send_to_rabbitmq(xml_message)
        return order

    def generate_xml_message(self, order):
        root = ET.Element("Order")
        ET.SubElement(root, "OrderID").text = str(order.id)
        ET.SubElement(root, "Customer").text = order.partner_id.name
        ET.SubElement(root, "TotalAmount").text = str(order.amount_total)
        return ET.tostring(root, encoding='unicode')

    def send_to_rabbitmq(self, message):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='orders', durable=True)
        channel.basic_publish(exchange='', routing_key='orders', body=message)
        print(" [x] Sent %r" % message)
        connection.close()
