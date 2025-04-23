import unittest
from unittest.mock import patch, MagicMock
from odoo.tests.common import TransactionCase
from datetime import datetime
import xml.etree.ElementTree as ET
import pika
from odoo.addons.user_delete.models.rabbitmq_publisher import USER_MESSAGE_XSD

class TestRabbitMQPublisher(TransactionCase):

    def setUp(self):
        super().setUp()
        # Get publisher model
        self.publisher = self.env['customer.delete.rabbitmq.publisher']
        
        # Create timestamp for external_id
        self.timestamp_id = '2023-04-23T10:20:30.123456Z'
        
        # Create test customer with timestamp external_id
        self.test_customer = self.env['res.partner'].create({
            'name': 'Test Delete Customer',
            'email': 'test.delete@example.com',
            'customer_rank': 1,
            'external_id': self.timestamp_id
        })

    def test_create_customer_delete_message(self):
        """Test the XML message creation for customer deletion"""
        # Create a message
        xml_message = self.publisher.create_customer_delete_message(
            self.test_customer.id, self.test_customer.external_id
        )
        
        # Parse the XML to validate structure
        root = ET.fromstring(xml_message)
        
        # Check root element name
        self.assertEqual(root.tag, 'UserMessage')
        
        # Check required elements exist
        self.assertEqual(root.find('ActionType').text, 'DELETE')
        self.assertEqual(root.find('UUID').text, self.timestamp_id)  # Should use timestamp external_id
        
        # Verify TimeOfAction is present and is in timestamp format
        time_of_action = root.find('TimeOfAction').text
        self.assertIsNotNone(time_of_action)
        self.assertIn('T', time_of_action)
        self.assertIn(':', time_of_action)
        self.assertIn('-', time_of_action)

    def test_validate_xml_against_xsd(self):
        """Test XML validation against XSD schema"""
        # Create a valid XML message with timestamp UUID
        valid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>DELETE</ActionType>
            <UUID>2023-04-23T10:20:30.123456Z</UUID>
            <TimeOfAction>2023-05-15T10:30:00.123456Z</TimeOfAction>
        </UserMessage>'''
        
        # Test validation passes
        is_valid = self.publisher.validate_xml_against_xsd(valid_xml, USER_MESSAGE_XSD)
        self.assertTrue(is_valid)
        
        # Create an invalid XML message (missing required element)
        invalid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>DELETE</ActionType>
            <TimeOfAction>2023-05-15T10:30:00.123456Z</TimeOfAction>
        </UserMessage>'''
        
        # Test validation fails for missing element
        is_valid = self.publisher.validate_xml_against_xsd(invalid_xml, USER_MESSAGE_XSD)
        self.assertFalse(is_valid)
        
        # Create an invalid XML message (invalid UUID format)
        invalid_uuid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>DELETE</ActionType>
            <UUID>not-a-timestamp</UUID>
            <TimeOfAction>2023-05-15T10:30:00.123456Z</TimeOfAction>
        </UserMessage>'''
        
        # Test validation fails for invalid UUID format
        is_valid = self.publisher.validate_xml_against_xsd(invalid_uuid_xml, USER_MESSAGE_XSD)
        self.assertFalse(is_valid)

    @patch('pika.BlockingConnection')
    def test_publish_customer_delete(self, mock_connection):
        """Test publishing message to RabbitMQ"""
        # Mock the RabbitMQ connection and channel
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        
        # Call the publish method
        result = self.publisher.publish_customer_delete(
            self.test_customer.id, self.test_customer.external_id
        )
        
        # Verify the connection was established
        mock_connection.assert_called_once()
        
        # Verify channel methods were called
        self.assertEqual(mock_channel.queue_declare.call_count, 3)  # Three queues
        self.assertEqual(mock_channel.queue_bind.call_count, 3)  # Three bindings
        self.assertEqual(mock_channel.basic_publish.call_count, 3)  # Three publications
        
        # Verify connection was closed
        mock_connection.return_value.close.assert_called_once()
        
        # Verify result
        self.assertTrue(result)

    def test_customer_unlink_triggers_publish(self):
        """Test that deleting a customer triggers the publish method"""
        with patch.object(self.env['customer.delete.rabbitmq.publisher'], 
                          'publish_customer_delete') as mock_publish:
            # Delete the test customer
            self.test_customer.unlink()
            
            # Verify the publish method was called with timestamp external_id
            mock_publish.assert_called_once_with(
                self.test_customer.id, self.test_customer.external_id
            )