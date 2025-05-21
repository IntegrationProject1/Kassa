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

DELETE_QUEUE = 'kassa_event_deleted'

DELETE_EVENT_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           elementFormDefault="qualified">
    <xs:element name="DeleteEvent">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="UUID" type="xs:dateTime"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
'''

def log_message(message):
    print(f"[EVENT_DELETE_CONSUMER] {message}")
    _logger.info(message)

class EventDeleteThread(threading.Thread):
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
                channel.queue_declare(queue=DELETE_QUEUE, durable=True)

                def callback(ch, method, properties, body):
                    log_message(f"Received delete message: {body[:100].decode()}")
                    try:
                        self._process_message(body)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        log_message("Delete message processed successfully")
                    except Exception as e:
                        log_message(f"Processing error: {e}")
                        traceback.print_exc()
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue=DELETE_QUEUE, on_message_callback=callback)
                log_message("Started consuming delete messages")
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
            schema = etree.XMLSchema(etree.fromstring(DELETE_EVENT_XSD.encode()))

            if not schema.validate(xml):
                error_details = "\n".join([f"Line {e.line}: {e.message}" for e in schema.error_log])
                log_message(f"XML validation failed:\n{error_details}")
                raise ValueError("Invalid XML structure")

            uuid = xml.findtext('UUID')
            log_message(f"Attempting to delete event with UUID: {uuid}")

            event = env['event.event'].search([('uuid', '=', uuid)], limit=1)
            if event:
                event.unlink()
                log_message(f"Event with UUID {uuid} successfully deleted.")
            else:
                log_message(f"No event found with UUID {uuid}")

# Global thread instance
event_delete_thread = None

class RabbitMQEventDelete(models.AbstractModel):
    _name = 'rabbitmq.event.delete'
    _description = 'RabbitMQ Event Delete Consumer'

    @api.model
    def start_service(self):
        global event_delete_thread
        if not event_delete_thread or not event_delete_thread.is_alive():
            log_message("Starting EventDeleteThread...")
            event_delete_thread = EventDeleteThread(self.env)
            event_delete_thread.start()
            return True
        log_message("EventDeleteThread already running.")
        return False

class RabbitMQEventDeleteStartup(models.AbstractModel):
    _name = "rabbitmq.event.delete.startup"
    _description = "Start RabbitMQ Event Delete on Odoo startup"

    @api.model
    def _register_hook(self):
        self.env['rabbitmq.event.delete'].start_service()
