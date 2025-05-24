import pika
import threading
import time
import logging
import traceback
import os
from datetime import datetime
from odoo import models, api
from lxml import etree
import json

_logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')

UPDATE_QUEUE = 'kassa_event_update'

UPDATE_EVENT_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           elementFormDefault="qualified">
    <xs:element name="UpdateEvent">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="EventUUID" type="xs:dateTime"/>
                <xs:element name="EventName" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="EventDescription" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="StartDateTime" type="xs:dateTime" minOccurs="0" maxOccurs="1"/>
                <xs:element name="EndDateTime" type="xs:dateTime" minOccurs="0" maxOccurs="1"/>
                <xs:element name="EventLocation" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="Organisator" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="Capacity" type="xs:positiveInteger" minOccurs="0" maxOccurs="1"/>
                <xs:element name="EventType" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="RegisteredUsers" minOccurs="0">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="User" maxOccurs="unbounded" minOccurs="0">
                                <xs:complexType>
                                    <xs:sequence>
                                        <xs:element name="UUID" type="xs:string" minOccurs="0" maxOccurs="1"/>
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
    print(f"[EVENT_UPDATE_CONSUMER] {message}")
    _logger.info(message)

FIELD_MAP = {
    'EventName': 'name',
    'EventDescription': 'event_description',
    'StartDateTime': 'start_datetime',
    'EndDateTime': 'end_datetime',
    'EventLocation': 'location',
    'Organisator': 'organisator',
    'Capacity': 'capacity',
    'EventType': 'event_type'
}

class EventUpdateThread(threading.Thread):
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
                ))

                channel = connection.channel()
                channel.queue_declare(queue=UPDATE_QUEUE, durable=True)

                def callback(ch, method, properties, body):
                    log_message(f"Received message: {body[:100].decode()}")
                    try:
                        self._process_message(body)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        log_message("Update message processed successfully")
                    except Exception as e:
                        log_message(f"Processing error: {e}")
                        traceback.print_exc()
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue=UPDATE_QUEUE, on_message_callback=callback)
                log_message("Started consuming update messages")
                channel.start_consuming()
            except Exception as e:
                log_message(f"Connection error: {e}")
                traceback.print_exc()
                time.sleep(10)

    def _process_message(self, body):
        registry = self.env.registry
        with registry.cursor() as new_cr:
            env = api.Environment(new_cr, self.env.uid, self.env.context)
            xml_str = body.decode('utf-8')
            xml = etree.fromstring(xml_str.encode())

            schema = etree.XMLSchema(etree.fromstring(UPDATE_EVENT_XSD.encode()))
            if not schema.validate(xml):
                error_details = "\n".join([f"Line {e.line}: {e.message}" for e in schema.error_log])
                log_message(f"Error: XML validation failed:\n{error_details}")
                raise ValueError("Invalid XML structure")
            uuid = xml.findtext('EventUUID')
            log_message(f"Processing update for event UUID: {uuid}")

            event = env['event.event'].search([('uuid', '=', uuid)], limit=1)
            if not event:
                raise ValueError(f"No event found with UUID {uuid}")

            updates = {}
            for child in xml:
                tag = child.tag
                if tag == 'EventUUID':
                    continue
                elif tag == 'RegisteredUsers':
                    user_uuids = [user.findtext('UUID') for user in child.findall('User') if user.findtext('UUID')]

                    if user_uuids:
                        partners = env['res.partner'].search([('external_id', 'in', user_uuids)])
                        if not partners:
                            raise ValueError(f"No matching res.partner records for UUIDs: {user_uuids}")
                        updates['registered_user_ids'] = [(6, 0, partners.ids)]
                        log_message(f"Setting registered_user_ids to partner IDs: {partners.ids}")
                    else:
                        # Explicitly clear all users if <RegisteredUsers> is empty
                        updates['registered_user_ids'] = [(6, 0, [])]
                        log_message("Clearing all registered_user_ids (empty RegisteredUsers)")

            event.write(updates)
            log_message(f"Successfully updated event with UUID: {uuid}")

# Global thread instance
event_update_thread = None

class RabbitMQEventUpdate(models.AbstractModel):
    _name = 'rabbitmq.event.update'
    _description = 'RabbitMQ Event Update Consumer'

    @api.model
    def start_service(self):
        global event_update_thread
        if not event_update_thread or not event_update_thread.is_alive():
            log_message("Starting EventUpdateThread...")
            event_update_thread = EventUpdateThread(self.env)
            event_update_thread.start()
            return True
        log_message("EventUpdateThread already running.")
        return False

class RabbitMQEventUpdateStartup(models.AbstractModel):
    _name = "rabbitmq.event.update.startup"
    _description = "Start RabbitMQ Event Update on Odoo startup"

    @api.model
    def _register_hook(self):
        self.env['rabbitmq.event.update'].start_service()
