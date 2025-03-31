import pika
import os
import xml.etree.ElementTree as ET
import xmlschema
from odoo import models, api, fields

# Define the XML schema for validation
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
                <xs:element name="Street" type="xs:string" minOccurs="0"/>
                <xs:element name="City" type="xs:string" minOccurs="0"/>
                <xs:element name="Country" type="xs:string" minOccurs="0"/>
                <xs:element name="Zip" type="xs:string" minOccurs="0"/>
                <xs:element name="State" type="xs:string" minOccurs="0"/>
                <xs:element name="Website" type="xs:string" minOccurs="0"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>'''

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT')

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

    def send_update_message(self, partner_data):
        """Send a RabbitMQ message with updated user information."""
        exchange_name = 'user'
        queue_name = 'kassa_user_update_test'
        try:
            # Convert partner_data to XML
            print("Converting partner data to XML...")
            root = ET.Element("UserMessage")
            for key, value in partner_data.items():
                if value is not None:
                    child = ET.SubElement(root, key)
                    child.text = str(value)

            xml_data = ET.tostring(root, encoding='utf-8')
            print(f"Generated XML: {xml_data.decode('utf-8')}")

            # Validate XML against the XSD schema
            self.validate_with_xsd(xml_data)

            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST,port=RABBITMQ_PORT ,  credentials=credentials))
            channel = connection.channel()

            # Ensure the exchange exists
            try:
                channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True, passive=True)
                print(f"Exchange '{exchange_name}' already exists.")
            except pika.exceptions.ChannelClosedByBroker:
                print(f"Exchange '{exchange_name}' does not exist. Creating it.")
                channel = connection.channel()  # Reopen the channel
                channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

            # Ensure the queue exists and bind it to the exchange
            try:
                channel.queue_declare(queue=queue_name, durable=True, passive=True)
                print(f"Queue '{queue_name}' already exists.")
            except pika.exceptions.ChannelClosedByBroker:
                print(f"Queue '{queue_name}' does not exist. Creating it.")
                channel = connection.channel()  # Reopen the channel
                channel.queue_declare(queue=queue_name, durable=True)

            # Bind the queue to the exchange
            channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=queue_name)

            # Publish the message to the exchange
            channel.basic_publish(exchange=exchange_name, routing_key=queue_name, body=xml_data)
            print(f"Message sent to exchange '{exchange_name}' with routing key '{queue_name}': {xml_data.decode('utf-8')}")

            # Close the connection
            connection.close()
        except Exception as e:
            print(f"Error sending RabbitMQ message: {e}")

    def write(self, vals):
        """Override the write method to send a RabbitMQ message on update."""
        print("Updating a partner...")
        # Call the original write method to update the user
        result = super(ResPartner, self).write(vals)

        # Prepare the updated partner data
        for partner in self:
            partner_data = {
                'ActionType': 'Update',
                'UserID': str(partner.id),
                'TimeOfAction': fields.Datetime.now().isoformat(),  # Use ISO 8601 format
                'FirstName': partner.name.split(' ')[0] if partner.name else '',
                'LastName': ' '.join(partner.name.split(' ')[1:]) if partner.name and ' ' in partner.name else '',
                'PhoneNumber': partner.phone or '',
                'EmailAddress': partner.email or '',
                'Street': partner.street or '',
                'City': partner.city or '',
                'Country': partner.country_id.name if partner.country_id else '',
                'Zip': partner.zip or '',
                'State': partner.state_id.name if partner.state_id else '',
                'Website': partner.website or '',
            }

            print(f"Partner data: {partner_data}")

            # Send the RabbitMQ message
            self.send_update_message(partner_data)

        return result