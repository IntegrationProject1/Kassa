import unittest
from unittest.mock import patch, MagicMock
from odoo.tests.common import TransactionCase
from odoo.addons.event_rabbitmq_consumer.models.event_create_consumer import EventCreateThread
from lxml import etree


class TestEventCreateConsumer(TransactionCase):

    def setUp(self):
        super().setUp()
        self.thread = EventCreateThread(self.env)

    def test_valid_event_create_message(self):
        """Test processing a valid CREATE event message"""
        valid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <CreateEvent>
            <EventUUID>2025-05-06T14:00:00.123456Z</EventUUID>
            <EventName>Tech Conference</EventName>
            <EventDescription>A conference for tech enthusiasts.</EventDescription>
            <StartDateTime>2025-05-10T09:00:00.000000Z</StartDateTime>
            <EndDateTime>2025-05-10T17:00:00.000000Z</EndDateTime>
            <EventLocation>Brussels Expo</EventLocation>
            <Organisator>TechOrg</Organisator>
            <Capacity>100</Capacity>
            <EventType>Conference</EventType>
            <RegisteredUsers>
                <User><UUID>2025-01-01T12:00:00.000000Z</UUID></User>
            </RegisteredUsers>
        </CreateEvent>
        '''

        test_partner = self.env['res.partner'].create({
            'name': 'Test User',
            'email': 'test@example.com',
            'external_id': '2025-01-01T12:00:00.000000Z'
        })

        with patch.object(self.env['event.event'], 'create') as mock_create:
            mock_create.return_value = MagicMock(id=123)
            self.thread._process_message(valid_xml.encode('utf-8'), 'kassa_event_create')

            mock_create.assert_called_once()
            create_vals = mock_create.call_args[0][0]
            self.assertEqual(create_vals['name'], 'Tech Conference')
            self.assertEqual(create_vals['location'], 'Brussels Expo')
            self.assertEqual(create_vals['capacity'], 100)
            self.assertIn(test_partner.id, create_vals.get('registered_user_ids')[0][2])

    def test_invalid_xml_format(self):
        """Test processing message with malformed XML"""
        invalid_xml = '''<CreateEvent><EventUUID>2025-05-06T14:00:00Z</EventUUID><EventName>Broken'''
        result = self.thread._process_message(invalid_xml.encode('utf-8'), 'kassa_event_create')
        self.assertIsNone(result)

    def test_invalid_datetime_format(self):
        """Test processing message with invalid datetime format"""
        invalid_datetime_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <CreateEvent>
            <EventUUID>invalid-datetime</EventUUID>
            <EventName>Test Event</EventName>
            <EventDescription>Desc</EventDescription>
            <StartDateTime>2025-05-10T09:00:00Z</StartDateTime>
            <EndDateTime>2025-05-10T17:00:00Z</EndDateTime>
            <EventLocation>Somewhere</EventLocation>
            <Organisator>Org</Organisator>
            <Capacity>50</Capacity>
            <EventType>Meetup</EventType>
        </CreateEvent>
        '''
        result = self.thread._process_message(invalid_datetime_xml.encode('utf-8'), 'kassa_event_create')
        self.assertIsNone(result)

    def test_schema_validation_failure(self):
        """Test processing message failing XSD validation (e.g., missing required tag)"""
        missing_tag_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <CreateEvent>
            <EventUUID>2025-05-06T14:00:00Z</EventUUID>
            <EventName>Test Event</EventName>
            <!-- Missing EventDescription -->
            <StartDateTime>2025-05-10T09:00:00Z</StartDateTime>
            <EndDateTime>2025-05-10T17:00:00Z</EndDateTime>
            <EventLocation>Somewhere</EventLocation>
            <Organisator>Org</Organisator>
            <Capacity>50</Capacity>
            <EventType>Meetup</EventType>
        </CreateEvent>
        '''
        result = self.thread._process_message(missing_tag_xml.encode('utf-8'), 'kassa_event_create')
        self.assertIsNone(result)
