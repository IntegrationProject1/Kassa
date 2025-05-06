# event_rabbitmq_consumer/models/event_create_consumer.py

import pika
import threading
import time
import logging
import traceback
import os
from datetime import datetime
from odoo import models, api
from lxml import etree

_logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')

SERVICE_QUEUES = ['event.created']

EVENT_CREATE_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           elementFormDefault="qualified">

  <xs:element name="Event">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="UUID" type="xs:dateTime"/>
        <xs:element name="Name" type="xs:string"/>
        <xs:element name="Description" type="xs:string"/>
        <xs:element name="StartDateTime" type="xs:dateTime"/>
        <xs:element name="EndDateTime" type="xs:dateTime"/>
        <xs:element name="Location" type="xs:string"/>
        <xs:element name="Organisator" type="xs:string"/>
        <xs:element name="Capacity" type="xs:positiveInteger"/>
        <xs:element name="EventType" type="xs:string"/>
        <xs:element name="RegisteredUsers" minOccurs="0">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="User" maxOccurs="unbounded">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="UUID" type="xs:dateTime"/>
                    <xs:element name="Name" type="xs:string"/>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
'''

def log_message(message):
    print(f"[EVENT_CREATE_CONSUMER] {message}")
    _logger.info(message)

def _format_uuid(raw_uuid):
    try:
        if isinstance(raw_uuid, str):
            return raw_uuid.replace('T', ' ').replace('Z', '')
        dt = datetime.fromisoformat(raw_uuid.replace('Z', '+00:00'))
        return dt.isoformat(sep=' ')
    except Exception:
        return raw_uuid

class EventCreateThread(threading.Thread):
    def __init__(self, env):
        super().__init__()
        self.env = env
        self.daemon = True
        self.running = True

    def run(self):
        log_message(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        while self.running:
            try:
                credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
                connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials,
                    heartbeat=600
                ))

                for queue in SERVICE_QUEUES:
                    channel = connection.channel()
                    channel.queue_declare(queue=queue, durable=True)

                    def make_callback(q):
                        def callback(ch, method, properties, body):
                            log_message(f"Received message from {q}: {body[:100].decode()}")
                            try:
                                self._process_message(body, q)
                                ch.basic_ack(delivery_tag=method.delivery_tag)
                                log_message("Message processed successfully")
                            except Exception as e:
                                log_message(f"Error: {e}")
                                traceback.print_exc()
                                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        return callback

                    channel.basic_qos(prefetch_count=1)
                    channel.basic_consume(queue=queue, on_message_callback=make_callback(queue))

                log_message("Started consuming")
                channel.start_consuming()
            except Exception as e:
                log_message(f"Connection error: {e}")
                traceback.print_exc()
                time.sleep(10)

    def _process_message(self, body, queue):
        registry = self.env.registry
        with registry.cursor() as new_cr:
            env = api.Environment(new_cr, self.env.uid, self.env.context)
            xml_str = body.decode('utf-8')
            xml = etree.fromstring(xml_str.encode())
            schema = etree.XMLSchema(etree.fromstring(EVENT_CREATE_XSD.encode()))
            if not schema.validate(xml):
                error_details = "\n".join([f"Line {e.line}: {e.message}" for e in schema.error_log])
                log_message(f"XML validation failed:\n{error_details}")
                raise ValueError("Invalid XML structure")

            uuid = xml.findtext('UUID')
            vals = {
                'uuid': uuid,
                'name': xml.findtext('Name'),
                'description': xml.findtext('Description'),
                'start_datetime': xml.findtext('StartDateTime'),
                'end_datetime': xml.findtext('EndDateTime'),
                'location': xml.findtext('Location'),
                'organisator': xml.findtext('Organisator'),
                'capacity': int(xml.findtext('Capacity')),
                'event_type': xml.findtext('EventType'),
                'is_invoiced': False,  # Standaard niet gefactureerd bij aanmaken
            }

            user_ids = []
            users_el = xml.find('RegisteredUsers')
            if users_el is not None:
                for user in users_el.findall('User'):
                    user_uuid = user.findtext('UUID')
                    partner = env['res.partner'].search([('external_id', '=', user_uuid)], limit=1)
                    if partner:
                        user_ids.append(partner.id)
            if user_ids:
                vals['registered_user_ids'] = [(6, 0, user_ids)]

            env['event.event'].create(vals)
            log_message(f"Created event with UUID: {uuid}")

# Global thread instance
event_create_thread = None

class RabbitMQEventCreate(models.AbstractModel):
    _name = 'rabbitmq.event.create'
    _description = 'RabbitMQ Event Create Consumer'

    @api.model
    def start_service(self):
        global event_create_thread
        if not event_create_thread or not event_create_thread.is_alive():
            log_message("Starting EventCreateThread...")
            event_create_thread = EventCreateThread(self.env)
            event_create_thread.start()
            return True
        log_message("EventCreateThread already running.")
        return False

class RabbitMQEventCreateStartup(models.AbstractModel):
    _name = "rabbitmq.event.create.startup"
    _description = "Start RabbitMQ Event Create on Odoo startup"

    @api.model
    def _register_hook(self):
        self.env['rabbitmq.event.create'].start_service()
