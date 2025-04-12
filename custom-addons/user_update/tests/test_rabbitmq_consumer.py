import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime
from odoo.tests.common import TransactionCase
from odoo.addons.user_update.models.rabbitmq_consumer import CustomerUpdateThread, XSD_SCHEMA

import xml.etree.ElementTree as ET


class TestUserUpdateConsumer(TransactionCase):

    def setUp(self):
        super().setUp()
        # Create a mock environment for the thread
        self.thread = CustomerUpdateThread(self.env)
        
        # Create a test partner
        self.test_partner = self.env['res.partner'].create({
            'name': 'Test Customer',
            'email': 'test@example.com',
            'phone': '+32123456789',
            'customer_rank': 1,
            'external_id': '54321'
        })

    def test_valid_update_message(self):
        """Test processing a valid UPDATE message"""
        # Create valid XML message
        valid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>UPDATE</ActionType>
            <UUID>54321</UUID>
            <TimeOfAction>2023-01-01T12:00:00</TimeOfAction>
            <FirstName>Updated</FirstName>
            <LastName>Customer</LastName>
            <PhoneNumber>+32987654321</PhoneNumber>
            <EmailAddress>updated@example.com</EmailAddress>
        </UserMessage>
        '''
        
        # Process the message
        result = self.thread._process_message(valid_xml.encode('utf-8'), 'test_queue')
        
        # Verify the result
        self.assertTrue(result)
        
        # Refresh the partner from the database
        self.test_partner.refresh()
        
        # Verify the customer was updated with the correct data
        self.assertEqual(self.test_partner.name, 'Updated Customer')
        self.assertEqual(self.test_partner.email, 'updated@example.com')
        self.assertEqual(self.test_partner.phone, '+32987654321')

    def test_update_with_business_data(self):
        """Test updating a customer with business data"""
        # Create XML with business information
        business_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>UPDATE</ActionType>
            <UUID>54321</UUID>
            <TimeOfAction>2023-01-01T12:00:00</TimeOfAction>
            <FirstName>Business</FirstName>
            <LastName>Customer</LastName>
            <Business>
                <BusinessName>Test Company BV</BusinessName>
                <BusinessEmail>company@example.com</BusinessEmail>
                <RealAddress>Business Street 123</RealAddress>
                <BTWNumber>BE0987654321</BTWNumber>
                <FacturationAddress>Finance Street 456</FacturationAddress>
            </Business>
        </UserMessage>
        '''
        
        # Process the message
        result = self.thread._process_message(business_xml.encode('utf-8'), 'test_queue')
        
        # Verify the result
        self.assertTrue(result)
        
        # Refresh the partner from the database
        self.test_partner.refresh()
        
        # Verify customer was updated with business information
        self.assertEqual(self.test_partner.name, 'Business Customer')
        self.assertEqual(self.test_partner.email, 'company@example.com')
        self.assertEqual(self.test_partner.street, 'Business Street 123')
        self.assertEqual(self.test_partner.street2, 'Finance Street 456')
        self.assertEqual(self.test_partner.vat, 'BE0987654321')
        
        # Check if company was created and linked
        self.assertTrue(self.test_partner.parent_id.exists())
        self.assertEqual(self.test_partner.parent_id.name, 'Test Company BV')

    def test_invalid_xml_format(self):
        """Test processing invalid XML format"""
        invalid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>UPDATE</ActionType>
            <UUID>54321</UUID>
            <TimeOfAction>2023-01-01T12:00:00
        </UserMessage>
        '''
        
        # Process the message
        result = self.thread._process_message(invalid_xml.encode('utf-8'), 'test_queue')
        
        # Verify validation fails
        self.assertFalse(result)

    def test_missing_required_elements(self):
        """Test processing XML with missing required elements"""
        missing_elem_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>UPDATE</ActionType>
            <TimeOfAction>2023-01-01T12:00:00</TimeOfAction>
            <FirstName>Missing</FirstName>
            <LastName>UUID</LastName>
        </UserMessage>
        '''
        
        # Process the message
        result = self.thread._process_message(missing_elem_xml.encode('utf-8'), 'test_queue')
        
        # Verify validation fails
        self.assertFalse(result)

    def test_non_integer_uuid(self):
        """Test processing UUID that is not an integer"""
        non_int_uuid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>UPDATE</ActionType>
            <UUID>abc123</UUID>
            <TimeOfAction>2023-01-01T12:00:00</TimeOfAction>
        </UserMessage>
        '''
        
        # Process the message
        result = self.thread._process_message(non_int_uuid_xml.encode('utf-8'), 'test_queue')
        
        # Verify validation fails
        self.assertFalse(result)

    def test_updating_nonexistent_customer(self):
        """Test updating a non-existent customer"""
        nonexistent_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UserMessage>
            <ActionType>UPDATE</ActionType>
            <UUID>999999</UUID>
            <TimeOfAction>2023-01-01T12:00:00</TimeOfAction>
            <FirstName>Non</FirstName>
            <LastName>Existent</LastName>
        </UserMessage>
        '''
        
        # Process the message
        result = self.thread._process_message(nonexistent_xml.encode('utf-8'), 'test_queue')
        
        # Verify update fails for non-existent customer
        self.assertFalse(result)