import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime
from odoo.tests.common import TransactionCase
from odoo.addons.user_update.models.publisher_user_update import USER_UPDATE_XSD, TARGET_QUEUES, EXCHANGE_NAME
import xml.etree.ElementTree as ET
from lxml import etree

class TestUserUpdatePublisher(TransactionCase):

    def setUp(self):
        super().setUp()
        # Create a test partner with timestamp-based external_id
        self.timestamp_id = '2023-04-23T10:20:30.123456Z'
        self.test_partner = self.env['res.partner'].create({
            'name': 'Test Customer',
            'email': 'test@example.com',
            'phone': '+32123456789',
            'customer_rank': 1,
            'external_id': self.timestamp_id
        })

    def test_create_customer_update_message(self):
        """Test creation of XML message for customer update"""
        partner_data = {
            'ActionType': 'UPDATE',
            'UUID': self.timestamp_id,
            'TimeOfAction': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'EncryptedPassword': 'odooadmin',
            'FirstName': 'Test',
            'LastName': 'Customer',
            'PhoneNumber': '+32123456789',
            'EmailAddress': 'test.updated@example.com',
            'Business': {
                'BusinessName': 'Test Company',
                'BusinessEmail': 'business@example.com',
                'RealAddress': 'Main Street 123',
                'BTWNumber': 'BE0123456789',
                'FacturationAddress': 'Finance Avenue 456'
            }
        }
        
        xml_string = self.test_partner.create_customer_update_message(partner_data)
        
        # Parse the XML
        root = ET.fromstring(xml_string)
        
        # Check root element name
        self.assertEqual(root.tag, 'UserMessage')
        
        # Check required elements
        self.assertEqual(root.find('ActionType').text, 'UPDATE')
        self.assertEqual(root.find('UUID').text, self.timestamp_id)
        
        # Check regular elements
        self.assertEqual(root.find('EncryptedPassword').text, 'odooadmin')
        self.assertEqual(root.find('FirstName').text, 'Test')
        self.assertEqual(root.find('LastName').text, 'Customer')
        self.assertEqual(root.find('PhoneNumber').text, '+32123456789')
        self.assertEqual(root.find('EmailAddress').text, 'test.updated@example.com')
        
        # Check business elements
        business = root.find('Business')
        self.assertIsNotNone(business)
        self.assertEqual(business.find('BusinessName').text, 'Test Company')
        self.assertEqual(business.find('BusinessEmail').text, 'business@example.com')
        self.assertEqual(business.find('RealAddress').text, 'Main Street 123')
        self.assertEqual(business.find('BTWNumber').text, 'BE0123456789')
        self.assertEqual(business.find('FacturationAddress').text, 'Finance Avenue 456')

    def test_validate_xml_against_xsd(self):
        """Test XML validation against XSD schema"""
        # Create a valid XML message with timestamp UUID
        valid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>UPDATE</ActionType>
            <UUID>2023-05-15T10:30:00.123456Z</UUID>
            <TimeOfAction>2023-05-15T10:30:00.123456Z</TimeOfAction>
            <EncryptedPassword>odooadmin</EncryptedPassword>
            <FirstName>Test</FirstName>
            <LastName>Customer</LastName>
        </UserMessage>'''
        
        # Test validation passes
        is_valid = self.test_partner.validate_xml_against_xsd(valid_xml, USER_UPDATE_XSD)
        self.assertTrue(is_valid)
        
        # Create an invalid XML message (missing required element)
        invalid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>UPDATE</ActionType>
            <TimeOfAction>2023-05-15T10:30:00.123456Z</TimeOfAction>
            <EncryptedPassword>odooadmin</EncryptedPassword>
        </UserMessage>'''
        
        # Test validation fails
        is_valid = self.test_partner.validate_xml_against_xsd(invalid_xml, USER_UPDATE_XSD)
        self.assertFalse(is_valid)
        
        # Create an invalid XML message (invalid UUID format)
        invalid_uuid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>UPDATE</ActionType>
            <UUID>not-a-valid-datetime</UUID>
            <TimeOfAction>2023-05-15T10:30:00.123456Z</TimeOfAction>
            <EncryptedPassword>odooadmin</EncryptedPassword>
        </UserMessage>'''
        
        # Test validation fails for invalid UUID format
        is_valid = self.test_partner.validate_xml_against_xsd(invalid_uuid_xml, USER_UPDATE_XSD)
        self.assertFalse(is_valid)

    @patch('pika.BlockingConnection')
    def test_publish_customer_update(self, mock_connection):
        """Test publishing message to RabbitMQ"""
        # Mock the RabbitMQ connection and channel
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        
        # Partner data for update with timestamp UUID
        partner_data = {
            'ActionType': 'UPDATE',
            'UUID': self.timestamp_id,
            'TimeOfAction': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'EncryptedPassword': 'odooadmin',
            'FirstName': 'Test',
            'LastName': 'Customer',
            'PhoneNumber': '+32123456789',
            'EmailAddress': 'test.updated@example.com'
        }
        
        # Call the publish method
        result = self.test_partner.publish_customer_update(partner_data)
        
        # Verify the connection was established
        mock_connection.assert_called_once()
        
        # Verify exchange declaration
        mock_channel.exchange_declare.assert_called_once_with(
            exchange=EXCHANGE_NAME, exchange_type='topic', durable=True
        )
        
        # Verify queue operations - should be called for each TARGET_QUEUE
        self.assertEqual(mock_channel.queue_declare.call_count, len(TARGET_QUEUES))
        self.assertEqual(mock_channel.queue_bind.call_count, len(TARGET_QUEUES))
        self.assertEqual(mock_channel.basic_publish.call_count, len(TARGET_QUEUES))
        
        # Verify connection was closed
        mock_connection.return_value.close.assert_called_once()
        
        # Verify result
        self.assertTrue(result)

    def test_write_triggers_publish(self):
        """Test that writing to a customer triggers publish_customer_update"""
        with patch.object(self.env['res.partner'], 'publish_customer_update') as mock_publish:
            # Update an existing customer
            self.test_partner.with_context(skip_rabbitmq_message=False).write({
                'email': 'updated@example.com',
                'phone': '+32987654321'
            })
            
            # Verify publish was called
            mock_publish.assert_called_once()
            
            # Check the parameters passed to publish_customer_update
            call_args = mock_publish.call_args[0][0]
            self.assertEqual(call_args['ActionType'], 'UPDATE')
            self.assertEqual(call_args['UUID'], self.timestamp_id)
            self.assertEqual(call_args['EmailAddress'], 'updated@example.com')
            self.assertEqual(call_args['PhoneNumber'], '+32987654321')