import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime
from odoo.tests.common import TransactionCase
from odoo.addons.user_delete.models.rabbitmq_consumer import UserDeleteThread, XSD_SCHEMA

import xml.etree.ElementTree as ET


class TestUserDeleteConsumer(TransactionCase):

    def setUp(self):
        super().setUp()
        # Create a mock environment for the thread
        self.thread = UserDeleteThread(self.env)
        
        # Create a test customer with timestamp-based external_id
        self.test_customer = self.env['res.partner'].create({
            'name': 'Test Customer for Deletion',
            'email': 'test@example.com',
            'external_id': '2023-04-23T10:20:30.123456Z'  # Updated to timestamp format
        })

    def test_valid_delete_message(self):
        """Test processing a valid DELETE message"""
        # Create valid XML message with timestamp UUID
        valid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>DELETE</ActionType>
            <UUID>2023-04-23T10:20:30.123456Z</UUID>
            <TimeOfAction>2023-04-23T10:20:30.123456Z</TimeOfAction>
        </UserMessage>
        '''
        
        # Mock cursor and environment
        with patch('odoo.api.Environment') as mock_env:
            # Setup the mock environment to return our test data
            mock_cr = MagicMock()
            mock_env.return_value = self.env
            
            # Process the message
            result = self.thread._process_message(valid_xml.encode('utf-8'), 'test_queue')
            
            # Verify the result
            self.assertTrue(result)
            
            # Verify the customer was searched with the correct external_id
            # And should have been archived and deleted

    def test_invalid_xml_format(self):
        """Test processing invalid XML format"""
        invalid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>DELETE</ActionType>
            <UUID>2023-04-23T10:20:30.123456Z</UUID>
            <TimeOfAction>2023-04-23T10:20:30
        </UserMessage>
        '''
        
        result = self.thread._process_message(invalid_xml.encode('utf-8'), 'test_queue')
        self.assertFalse(result)

    def test_non_delete_action(self):
        """Test processing action other than DELETE"""
        non_delete_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>UPDATE</ActionType>
            <UUID>2023-04-23T10:20:30.123456Z</UUID>
            <TimeOfAction>2023-04-23T10:20:30.123456Z</TimeOfAction>
        </UserMessage>
        '''
        
        result = self.thread._process_message(non_delete_xml.encode('utf-8'), 'test_queue')
        self.assertFalse(result)

    def test_missing_elements(self):
        """Test processing XML with missing elements"""
        missing_elem_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>DELETE</ActionType>
            <TimeOfAction>2023-04-23T10:20:30.123456Z</TimeOfAction>
        </UserMessage>
        '''
        
        result = self.thread._process_message(missing_elem_xml.encode('utf-8'), 'test_queue')
        self.assertFalse(result)

    def test_invalid_datetime_format(self):
        """Test processing UUID that is not a valid dateTime format"""
        invalid_datetime_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>DELETE</ActionType>
            <UUID>not-a-valid-datetime</UUID>
            <TimeOfAction>2023-04-23T10:20:30.123456Z</TimeOfAction>
        </UserMessage>
        '''
        
        result = self.thread._process_message(invalid_datetime_xml.encode('utf-8'), 'test_queue')
        self.assertFalse(result)

    def test_admin_user_protection(self):
        """Test that admin users (ID ≤ 2) are protected from deletion"""
        # Create test admin user with timestamp external_id
        admin_timestamp = '2023-04-23T11:22:33.445566Z'
        
        # Create XML message targeting admin user
        admin_delete_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>DELETE</ActionType>
            <UUID>{admin_timestamp}</UUID>
            <TimeOfAction>2023-04-23T10:20:30.123456Z</TimeOfAction>
        </UserMessage>
        '''
        
        # Mock cursor and environment
        with patch('odoo.api.Environment') as mock_env:
            # Setup mock to return admin user
            mock_cr = MagicMock()
            admin_user = MagicMock()
            admin_user.id = 1
            admin_user.name = "Admin"
            admin_user.email = "admin@example.com"
            admin_user.external_id = admin_timestamp
            
            mock_env_obj = MagicMock()
            mock_env_obj.__getitem__.return_value.sudo.return_value.search.return_value = admin_user
            mock_env.return_value = mock_env_obj
            
            # Process the message
            result = self.thread._process_message(admin_delete_xml.encode('utf-8'), 'test_queue')
            
            # Verify admin deletion was rejected
            self.assertFalse(result)
            # Verify the admin was not deleted or archived
            admin_user.write.assert_not_called()
            admin_user.unlink.assert_not_called()