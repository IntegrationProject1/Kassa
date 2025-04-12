import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime
from odoo.tests.common import TransactionCase
from odoo.addons.user_create.models.publisher_user_create import USER_CREATE_XSD, TARGET_QUEUES, EXCHANGE_NAME
import xml.etree.ElementTree as ET
from lxml import etree

class TestUserCreatePublisher(TransactionCase):

    def setUp(self):
        super().setUp()
        # Create a test partner
        self.test_partner = self.env['res.partner'].create({
            'name': 'Test Customer',
            'email': 'test@example.com',
            'phone': '+32123456789',
            'customer_rank': 1,
            'external_id': '54321'
        })

    def test_create_customer_create_message(self):
        """Test creation of XML message for customer creation"""
        partner_data = {
            'ActionType': 'CREATE',
            'UUID': 12345,
            'TimeOfAction': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            'FirstName': 'John',
            'LastName': 'Doe',
            'PhoneNumber': '+32123456789',
            'EmailAddress': 'john.doe@example.com',
            'Business': {
                'BusinessName': 'Doe Enterprises',
                'BusinessEmail': 'info@doe-enterprises.com',
                'RealAddress': 'Main Street 123',
                'BTWNumber': 'BE0123456789',
                'FacturationAddress': 'Finance Avenue 456'
            }
        }
        
        xml_string = self.test_partner.create_customer_create_message(partner_data)
        
        # Parse the XML
        root = ET.fromstring(xml_string)
        
        # Check root element name
        self.assertEqual(root.tag, 'UserMessage')
        
        # Check regular elements
        self.assertEqual(root.find('ActionType').text, 'CREATE')
        self.assertEqual(root.find('UUID').text, '12345')
        self.assertEqual(root.find('FirstName').text, 'John')
        self.assertEqual(root.find('LastName').text, 'Doe')
        self.assertEqual(root.find('PhoneNumber').text, '+32123456789')
        self.assertEqual(root.find('EmailAddress').text, 'john.doe@example.com')
        
        # Check business elements
        business = root.find('Business')
        self.assertIsNotNone(business)
        self.assertEqual(business.find('BusinessName').text, 'Doe Enterprises')
        self.assertEqual(business.find('BusinessEmail').text, 'info@doe-enterprises.com')
        self.assertEqual(business.find('RealAddress').text, 'Main Street 123')
        self.assertEqual(business.find('BTWNumber').text, 'BE0123456789')
        self.assertEqual(business.find('FacturationAddress').text, 'Finance Avenue 456')

    def test_validate_xml_against_xsd(self):
        """Test XML validation against XSD schema"""
        # Create a valid XML message
        valid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>CREATE</ActionType>
            <UUID>12345</UUID>
            <TimeOfAction>2023-05-15T10:30:00Z</TimeOfAction>
            <FirstName>John</FirstName>
            <LastName>Doe</LastName>
        </UserMessage>'''
        
        # Test validation passes
        is_valid = self.test_partner.validate_xml_against_xsd(valid_xml, USER_CREATE_XSD)
        self.assertTrue(is_valid)
        
        # Create an invalid XML message (missing required element)
        invalid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>CREATE</ActionType>
            <TimeOfAction>2023-05-15T10:30:00Z</TimeOfAction>
        </UserMessage>'''
        
        # Test validation fails
        is_valid = self.test_partner.validate_xml_against_xsd(invalid_xml, USER_CREATE_XSD)
        self.assertFalse(is_valid)

    @patch('pika.BlockingConnection')
    def test_publish_customer_create(self, mock_connection):
        """Test publishing message to RabbitMQ"""
        # Mock the RabbitMQ connection and channel
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        
        # Sample partner data
        partner_data = {
            'ActionType': 'CREATE',
            'UUID': 54321,
            'TimeOfAction': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            'FirstName': 'Test',
            'LastName': 'Customer',
            'PhoneNumber': '+32123456789',
            'EmailAddress': 'test@example.com'
        }
        
        # Call the publish method
        result = self.test_partner.publish_customer_create(partner_data)
        
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

    @patch('odoo.addons.user_create.models.publisher_user_create.ResPartner.publish_customer_create')
    def test_create_triggers_publish(self, mock_publish):
        """Test that creating a customer triggers publish_customer_create"""
        # Create a new partner
        new_partner = self.env['res.partner'].with_context(skip_rabbitmq_publish=False).create({
            'name': 'New Test Customer',
            'email': 'new.test@example.com',
            'customer_rank': 1
        })
        
        # Verify publish was called
        mock_publish.assert_called_once()
        
        # Check auto-generated external_id
        self.assertIsNotNone(new_partner.external_id)

    def test_auto_external_id_generation(self):
        """Test that external_id is automatically generated for new customers"""
        # Create several partners to verify incremental IDs
        partner1 = self.env['res.partner'].with_context(skip_rabbitmq_publish=True).create({
            'name': 'Auto ID Test 1',
            'customer_rank': 1
        })
        
        partner2 = self.env['res.partner'].with_context(skip_rabbitmq_publish=True).create({
            'name': 'Auto ID Test 2',
            'customer_rank': 1
        })
        
        # Verify both have external_ids and they're different
        self.assertIsNotNone(partner1.external_id)
        self.assertIsNotNone(partner2.external_id)
        self.assertNotEqual(partner1.external_id, partner2.external_id)
        
        # Verify the second ID is greater than the first
        self.assertGreater(int(partner2.external_id), int(partner1.external_id))