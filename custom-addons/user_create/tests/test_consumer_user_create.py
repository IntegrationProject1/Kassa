import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime
from odoo.tests.common import TransactionCase
from odoo.addons.user_create.models.consumer_user_create import CustomerCreateThread, XSD_SCHEMA

import xml.etree.ElementTree as ET


class TestUserCreateConsumer(TransactionCase):

    def setUp(self):
        super().setUp()
        # Create a mock environment for the thread
        self.thread = CustomerCreateThread(self.env)
        
        # Create a test partner to test update operations
        self.test_partner = self.env['res.partner'].create({
            'name': 'Existing Test Customer',
            'email': 'existing@example.com',
            'external_id': '2023-04-23T12:34:56.789012Z'  # Updated to timestamp format
        })

    def test_valid_create_message(self):
        """Test processing a valid CREATE message with basic user data"""
        # Create valid XML message with timestamp UUID
        valid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>CREATE</ActionType>
            <UUID>2023-04-23T10:20:30.123456Z</UUID>
            <TimeOfAction>2023-04-23T10:20:30.123456Z</TimeOfAction>
            <EncryptedPassword>odooadmin</EncryptedPassword>
            <FirstName>John</FirstName>
            <LastName>Doe</LastName>
            <PhoneNumber>+1234567890</PhoneNumber>
            <EmailAddress>john.doe@example.com</EmailAddress>
        </UserMessage>
        '''
        
        # Process the message
        with patch.object(self.env['res.partner'], 'create') as mock_create:
            mock_create.return_value = MagicMock(id=999, name="John Doe")
            result = self.thread._process_message(valid_xml.encode('utf-8'), 'test_queue')
            
            # Verify the result
            self.assertTrue(result)
            
            # Check that partner.create was called with correct values
            mock_create.assert_called_once()
            create_vals = mock_create.call_args[0][0]
            self.assertEqual(create_vals.get('name'), 'John Doe')
            self.assertEqual(create_vals.get('email'), 'john.doe@example.com')
            self.assertEqual(create_vals.get('phone'), '+1234567890')
            self.assertEqual(create_vals.get('external_id'), '2023-04-23T10:20:30.123456Z')

    def test_valid_create_message_with_business(self):
        """Test processing a valid CREATE message with business data"""
        # Create valid XML message with timestamp UUID and business information
        valid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>CREATE</ActionType>
            <UUID>2023-04-23T11:22:33.445566Z</UUID>
            <TimeOfAction>2023-04-23T11:22:33.445566Z</TimeOfAction>
            <EncryptedPassword>odooadmin</EncryptedPassword>
            <FirstName>Jane</FirstName>
            <LastName>Smith</LastName>
            <PhoneNumber>+9876543210</PhoneNumber>
            <EmailAddress>jane.smith@example.com</EmailAddress>
            <Business>
                <BusinessName>Smith Enterprises</BusinessName>
                <BusinessEmail>info@smith-enterprises.com</BusinessEmail>
                <RealAddress>123 Main Street</RealAddress>
                <BTWNumber>BE0123456789</BTWNumber>
                <FacturationAddress>456 Finance Avenue</FacturationAddress>
            </Business>
        </UserMessage>
        '''
        
        # Process the message
        with patch.object(self.env['res.partner'], 'create') as mock_create:
            mock_create.return_value = MagicMock(id=888, name="Jane Smith")
            result = self.thread._process_message(valid_xml.encode('utf-8'), 'test_queue')
            
            # Verify the result
            self.assertTrue(result)
            
            # Check that partner.create was called with correct values including business data
            mock_create.assert_called_once()
            create_vals = mock_create.call_args[0][0]
            self.assertEqual(create_vals.get('name'), 'Jane Smith')
            self.assertEqual(create_vals.get('email'), 'info@smith-enterprises.com')  # Business email should override personal
            self.assertEqual(create_vals.get('company_name'), 'Smith Enterprises')
            self.assertEqual(create_vals.get('vat'), 'BE0123456789')
            self.assertEqual(create_vals.get('street'), '123 Main Street')
            self.assertEqual(create_vals.get('street2'), '456 Finance Avenue')
            self.assertTrue(create_vals.get('is_company'))

    def test_invalid_xml_format(self):
        """Test processing invalid XML format"""
        invalid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>CREATE</ActionType>
            <UUID>2023-04-23T10:20:30.123456Z</UUID>
            <TimeOfAction>2023-04-23T10:20:30
        </UserMessage>
        '''
        
        result = self.thread._process_message(invalid_xml.encode('utf-8'), 'test_queue')
        self.assertFalse(result)

    def test_missing_required_elements(self):
        """Test processing XML with missing required elements"""
        missing_elem_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>CREATE</ActionType>
            <TimeOfAction>2023-04-23T10:20:30.123456Z</TimeOfAction>
            <EncryptedPassword>odooadmin</EncryptedPassword>
            <FirstName>John</FirstName>
            <LastName>Doe</LastName>
        </UserMessage>
        '''
        
        result = self.thread._process_message(missing_elem_xml.encode('utf-8'), 'test_queue')
        self.assertFalse(result)

    def test_invalid_datetime_format(self):
        """Test processing UUID that is not a valid dateTime format"""
        invalid_datetime_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>CREATE</ActionType>
            <UUID>not-a-valid-datetime</UUID>
            <TimeOfAction>2023-04-23T10:20:30.123456Z</TimeOfAction>
            <EncryptedPassword>odooadmin</EncryptedPassword>
            <FirstName>John</FirstName>
            <LastName>Doe</LastName>
        </UserMessage>
        '''
        
        result = self.thread._process_message(invalid_datetime_xml.encode('utf-8'), 'test_queue')
        self.assertFalse(result)