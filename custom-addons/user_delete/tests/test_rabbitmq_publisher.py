import unittest
from unittest.mock import patch, MagicMock
from odoo.tests.common import TransactionCase
import xml.etree.ElementTree as ET
import pika

class TestRabbitMQPublisher(TransactionCase):

    def setUp(self):
        super().setUp()
        # Get publisher model
        self.publisher = self.env['customer.delete.rabbitmq.publisher']
        # Create test customer
        self.test_customer = self.env['res.partner'].create({
            'name': 'Test Delete Customer',
            'email': 'test.delete@example.com',
            'customer_rank': 1,
            'external_id': '12345'
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
        self.assertEqual(root.find('UUID').text, '12345')  # Should use external_id
        
        # Verify TimeOfAction is present (exact value will vary)
        self.assertIsNotNone(root.find('TimeOfAction').text)

    def test_validate_xml_against_xsd(self):
        """Test XML validation against XSD schema"""
        # Create a valid XML message
        valid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>DELETE</ActionType>
            <UUID>12345</UUID>
            <TimeOfAction>2023-05-15T10:30:00Z</TimeOfAction>
        </UserMessage>'''
        
        # Test validation passes
        is_valid = self.publisher.validate_xml_against_xsd(valid_xml, self.publisher._name)
        self.assertTrue(is_valid)
        
        # Create an invalid XML message (missing required element)
        invalid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>DELETE</ActionType>
            <TimeOfAction>2023-05-15T10:30:00Z</TimeOfAction>
        </UserMessage>'''
        
        # Test validation fails
        is_valid = self.publisher.validate_xml_against_xsd(invalid_xml, self.publisher._name)
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
            
            # Verify the publish method was called
            mock_publish.assert_called_once_with(
                self.test_customer.id, self.test_customer.external_id
            )