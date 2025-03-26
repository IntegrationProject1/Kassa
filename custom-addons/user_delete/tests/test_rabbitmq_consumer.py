import unittest
from unittest.mock import patch, MagicMock, mock_open
import xml.etree.ElementTree as ET
from lxml import etree
import datetime
from odoo.tests import common
from odoo.exceptions import UserError, MissingError
from odoo.addons.user_delete.models.rabbitmq_consumer import (
    UserDeleteThread, XSD_SCHEMA, log_message, RabbitMQUserDelete
)


class TestUserDeleteThread(common.TransactionCase):
    """Test cases for the UserDeleteThread class."""
    
    def setUp(self):
        super(TestUserDeleteThread, self).setUp()
        # Create a mock environment
        self.thread = UserDeleteThread(self.env)
        
        # Create test user
        self.test_user = self.env['res.users'].create({
            'name': 'Test User For Deletion',
            'login': 'test_delete_user@example.com',
            'email': 'test_delete_user@example.com',
            'groups_id': [(6, 0, [self.ref('base.group_user')])],
        })
        
        # Valid XML message for testing
        self.valid_xml = f'''<UserMessage>
            <ActionType>DELETE</ActionType>
            <UserID>{self.test_user.id}</UserID>
            <TimeOfAction>2025-03-24T14:00:00Z</TimeOfAction>
        </UserMessage>'''.encode('utf-8')
        
        # Invalid XML for testing
        self.invalid_xml = b'''<UserMessage>
            <ActionType>DELETE</ActionType>
            <TimeOfAction>2025-03-24T14:00:00Z</TimeOfAction>
        </UserMessage>'''
        
        # XML with non-DELETE action
        self.non_delete_xml = b'''<UserMessage>
            <ActionType>UPDATE</ActionType>
            <UserID>100</UserID>
            <TimeOfAction>2025-03-24T14:00:00Z</TimeOfAction>
        </UserMessage>'''
        
        # Invalid XML schema
        self.invalid_schema_xml = b'''<WrongRoot>
            <ActionType>DELETE</ActionType>
            <UserID>100</UserID>
            <TimeOfAction>2025-03-24T14:00:00Z</TimeOfAction>
        </WrongRoot>'''

    def test_xml_schema_validation_success(self):
        """Test that valid XML passes schema validation."""
        message_str = self.valid_xml.decode('utf-8')
        schema_doc = etree.fromstring(XSD_SCHEMA.encode('utf-8'))
        schema = etree.XMLSchema(schema_doc)
        xml_doc = etree.fromstring(message_str.encode('utf-8'))
        self.assertTrue(schema.validate(xml_doc), "Valid XML should pass schema validation")

    def test_xml_schema_validation_failure(self):
        """Test that invalid XML fails schema validation."""
        message_str = self.invalid_schema_xml.decode('utf-8')
        schema_doc = etree.fromstring(XSD_SCHEMA.encode('utf-8'))
        schema = etree.XMLSchema(schema_doc)
        xml_doc = etree.fromstring(message_str.encode('utf-8'))
        self.assertFalse(schema.validate(xml_doc), "Invalid XML should fail schema validation")

    @patch('odoo.addons.user_delete.models.rabbitmq_consumer.log_message')
    def test_process_message_with_valid_xml(self, mock_log):
        """Test processing a valid XML message."""
        result = self.thread._process_message(self.valid_xml)
        self.assertTrue(result, "Processing valid XML should return True")
        # Verify the user was archived
        test_user = self.env['res.users'].sudo().browse(self.test_user.id)
        self.assertFalse(test_user.exists(), "User should have been deleted")

    @patch('odoo.addons.user_delete.models.rabbitmq_consumer.log_message')
    def test_process_message_with_invalid_xml(self, mock_log):
        """Test processing an invalid XML message."""
        result = self.thread._process_message(self.invalid_xml)
        self.assertFalse(result, "Processing invalid XML should return False")

    @patch('odoo.addons.user_delete.models.rabbitmq_consumer.log_message')
    def test_process_message_with_non_delete_action(self, mock_log):
        """Test processing XML with non-DELETE action type."""
        result = self.thread._process_message(self.non_delete_xml)
        self.assertFalse(result, "Processing non-DELETE action XML should return False")

    @patch('odoo.addons.user_delete.models.rabbitmq_consumer.log_message')
    def test_process_message_with_nonexistent_user(self, mock_log):
        """Test processing XML for a user that doesn't exist."""
        # Create XML with non-existent user ID
        nonexistent_user_xml = b'''<UserMessage>
            <ActionType>DELETE</ActionType>
            <UserID>99999</UserID>
            <TimeOfAction>2025-03-24T14:00:00Z</TimeOfAction>
        </UserMessage>'''
        
        result = self.thread._process_message(nonexistent_user_xml)
        self.assertFalse(result, "Processing XML with non-existent user should return False")

    @patch('odoo.addons.user_delete.models.rabbitmq_consumer.log_message')
    def test_process_message_with_admin_user(self, mock_log):
        """Test processing XML trying to delete admin user."""
        # Create XML trying to delete admin (ID 2)
        admin_delete_xml = b'''<UserMessage>
            <ActionType>DELETE</ActionType>
            <UserID>2</UserID>
            <TimeOfAction>2025-03-24T14:00:00Z</TimeOfAction>
        </UserMessage>'''
        
        result = self.thread._process_message(admin_delete_xml)
        self.assertFalse(result, "Trying to delete admin user should return False")
        
        # Verify admin user still exists
        admin_user = self.env['res.users'].sudo().browse(2)
        self.assertTrue(admin_user.exists(), "Admin user should not be deleted")
        self.assertTrue(admin_user.active, "Admin user should remain active")

    @patch('pika.BlockingConnection')
    def test_rabbitmq_connection(self, mock_connection):
        """Test RabbitMQ connection is properly established."""
        # Mock the connection
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_conn.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn
        
        # Mock queue_info with message_count
        mock_queue_info = MagicMock()
        mock_queue_info.method.message_count = 5
        mock_channel.queue_declare.return_value = mock_queue_info
        
        # Test the connection test method
        result = self.env['rabbitmq.user.delete'].test_connection()
        
        # Verify the connection was attempted with correct parameters
        mock_connection.assert_called_once()
        mock_channel.queue_declare.assert_called_with(queue='kassa_user_delete', durable=True)
        self.assertEqual(result['params']['type'], 'success')

    @patch('pika.BlockingConnection')
    def test_rabbitmq_connection_failure(self, mock_connection):
        """Test RabbitMQ connection failure handling."""
        # Mock the connection to raise an exception
        mock_connection.side_effect = Exception("Connection failed")
        
        # Test the connection test method with failure
        result = self.env['rabbitmq.user.delete'].test_connection()
        
        # Verify the result has error information
        self.assertEqual(result['params']['type'], 'danger')
        self.assertEqual(result['params']['title'], 'Connection Failed')

    @patch('odoo.addons.user_delete.models.rabbitmq_consumer.UserDeleteThread')
    def test_start_service(self, mock_thread):
        """Test starting the RabbitMQ user delete service."""
        # Create a mock thread
        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance
        
        # Start the service
        result = self.env['rabbitmq.user.delete'].start_service()
        
        # Verify thread was started
        mock_thread.assert_called_once_with(self.env)
        mock_thread_instance.start.assert_called_once()
        self.assertTrue(result)

    def test_stop_service(self):
        """Test stopping the RabbitMQ user delete service."""
        # We need to mock the global variable reference
        with patch('odoo.addons.user_delete.models.rabbitmq_consumer.user_delete_thread') as mock_thread:
            mock_thread.is_alive.return_value = True
            
            # Stop the service
            result = self.env['rabbitmq.user.delete'].stop_service()
            
            # Verify thread was stopped
            mock_thread.stop.assert_called_once()
            self.assertTrue(result)


class TestUserDeleteIntegration(common.TransactionCase):
    """Integration tests for user deletion process."""
    
    def setUp(self):
        super(TestUserDeleteIntegration, self).setUp()
        # Create a test user
        self.test_user = self.env['res.users'].create({
            'name': 'Integration Test User',
            'login': 'integration_test@example.com',
            'email': 'integration_test@example.com',
            'groups_id': [(6, 0, [self.ref('base.group_user')])],
        })
    
    @patch('odoo.addons.user_delete.models.rabbitmq_consumer.log_message')
    def test_full_user_delete_process(self, mock_log):
        """Test the complete user deletion process."""
        thread = UserDeleteThread(self.env)
        
        # Create valid XML for the test user
        valid_xml = f'''<UserMessage>
            <ActionType>DELETE</ActionType>
            <UserID>{self.test_user.id}</UserID>
            <TimeOfAction>{datetime.datetime.now().isoformat()}</TimeOfAction>
        </UserMessage>'''.encode('utf-8')
        
        # Process the message
        result = thread._process_message(valid_xml)
        self.assertTrue(result, "User deletion should succeed")
        
        # Verify user was deleted
        user = self.env['res.users'].sudo().browse(self.test_user.id)
        self.assertFalse(user.exists(), "User should have been deleted")