import pika
import os
import json
import xml.etree.ElementTree as ET
import xmlschema
from odoo import models, fields, api

XSD_SCHEMA = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="UserMessage">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="ActionType" type="xs:string"/>
                <xs:element name="UserID" type="xs:string"/>
                <xs:element name="TimeOfAction" type="xs:dateTime"/>
                <xs:element name="FirstName" type="xs:string" minOccurs="0"/>
                <xs:element name="LastName" type="xs:string" minOccurs="0"/>
                <xs:element name="PhoneNumber" type="xs:string" minOccurs="0"/>
                <xs:element name="EmailAddress" type="xs:string" minOccurs="0"/>
                <xs:element name="Business" minOccurs="0">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="BusinessName" type="xs:string" minOccurs="0"/>
                            <xs:element name="BusinessEmail" type="xs:string" minOccurs="0"/>
                            <xs:element name="RealAddress" type="xs:string" minOccurs="0"/>
                            <xs:element name="BTWNumber" type="xs:string" minOccurs="0"/>
                            <xs:element name="FacturationAddress" type="xs:string" minOccurs="0"/>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>'''

class ResPartner(models.Model):
    _inherit = 'res.partner'

    def validate_with_xsd(self, xml_data):
        """Validate XML data against the embedded XSD schema."""
        try:
            print("Validating XML data against XSD schema...")
            schema = xmlschema.XMLSchema(XSD_SCHEMA)
            schema.validate(xml_data)
            print("XML validation successful.")
        except xmlschema.validators.exceptions.XMLSchemaValidationError as e:
            print(f"XML validation error: {e}")
            raise ValueError("XML validation failed.")

    @api.model
    def createQueue(self):
        """Ensure the RabbitMQ exchange and queue exist, and bind them."""
        exchange_name = 'kassa'
        queue_name = 'kassa_user_create'
        try:
            print("Connecting to RabbitMQ...")
            rabbitmq_host = os.getenv('RABBITMQ_HOST')
            rabbitmq_user = os.getenv('RABBITMQ_USER')
            rabbitmq_password = os.getenv('RABBITMQ_PASSWORD')
            print(f"RabbitMQ Host: {rabbitmq_host}, User: {rabbitmq_user}")

            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()

            print("Checking if exchange exists...")
            try:
                channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True, passive=True)
                print(f"Exchange '{exchange_name}' already exists.")
            except pika.exceptions.ChannelClosedByBroker:
                print(f"Exchange '{exchange_name}' does not exist. Creating it.")
                channel = connection.channel()  # Reopen the channel
                channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

            print("Checking if queue exists...")
            try:
                channel.queue_declare(queue=queue_name, durable=True, passive=True)
                print(f"Queue '{queue_name}' already exists.")
            except pika.exceptions.ChannelClosedByBroker:
                print(f"Queue '{queue_name}' does not exist. Creating it.")
                channel = connection.channel()  # Reopen the channel
                channel.queue_declare(queue=queue_name, durable=True)

            print("Binding queue to exchange...")
            channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=queue_name)

            print(f"Exchange '{exchange_name}' and queue '{queue_name}' are set up successfully.")
            connection.close()
        except Exception as e:
            print(f"Error in createQueue: {e}")

    @api.model
    def create(self, vals):
        """Override the create method to send user data to RabbitMQ with XSD validation."""
        print("Creating a new partner...")
        partner = super(ResPartner, self).create(vals)

        exchange_name = 'kassa'
        queue_name = 'kassa_user_create'

        try:
            print("Serializing partner data...")
            partner_data = {
                'ActionType': 'Create',
                'UserID': str(partner.id),
                'TimeOfAction': fields.Datetime.now().isoformat(),  # Use ISO 8601 format
                'FirstName': partner.name.split(' ')[0] if partner.name else '',
                'LastName': ' '.join(partner.name.split(' ')[1:]) if partner.name and ' ' in partner.name else '',
                'PhoneNumber': partner.phone or '',
                'EmailAddress': partner.email or '',
                'Business': {
                    'BusinessName': partner.company_name or '',
                    'BusinessEmail': partner.email or '',
                    'RealAddress': partner.street or '',
                    'BTWNumber': vals.get('vat', ''),
                    'FacturationAddress': partner.street2 or '',
                } if partner.is_company else None,
            }

            print(f"Partner data: {partner_data}")

            print("Converting partner data to XML...")
            root = ET.Element("UserMessage")
            for key, value in partner_data.items():
                if isinstance(value, dict):
                    business_element = ET.SubElement(root, key)
                    for sub_key, sub_value in value.items():
                        if sub_value:
                            sub_child = ET.SubElement(business_element, sub_key)
                            sub_child.text = sub_value
                elif value:
                    child = ET.SubElement(root, key)
                    child.text = str(value)

            xml_data = ET.tostring(root, encoding='utf-8')
            print(f"Generated XML: {xml_data.decode('utf-8')}")

            print("Validating XML...")
            self.validate_with_xsd(xml_data)

            print("Connecting to RabbitMQ...")
            rabbitmq_host = os.getenv('RABBITMQ_HOST')
            rabbitmq_user = os.getenv('RABBITMQ_USER')
            rabbitmq_password = os.getenv('RABBITMQ_PASSWORD')
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()

            print("Ensuring exchange and queue exist...")
            self.createQueue()

            print("Publishing message to RabbitMQ...")
            channel.basic_publish(exchange=exchange_name, routing_key=queue_name, body=xml_data)
            print(f"Message published to exchange '{exchange_name}' with routing key '{queue_name}'.")

            connection.close()
        except Exception as e:
            print(f"Error in create method: {e}")
            raise

        return partner