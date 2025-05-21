import unittest
from unittest.mock import patch
from odoo.tests.common import TransactionCase
from odoo.addons.event_rabbitmq_consumer.models.event_update_consumer import EventUpdateThread


class TestEventUpdateConsumer(TransactionCase):

    def setUp(self):
        super().setUp()
        self.thread = EventUpdateThread(self.env)

        self.partner = self.env['res.partner'].create({
            'name': 'Updated User',
            'external_id': '2025-01-01T12:00:00.000000Z',
        })

        self.event = self.env['event.event'].create({
            'uuid': '2025-05-20T14:30:45.123456Z',
            'name': 'Original Event',
            'description': 'Old desc',
            'start_datetime': '2025-05-21T09:00:00.000000Z',
            'end_datetime': '2025-05-21T17:00:00.000000Z',
            'location': 'Old Hall',
            'organisator': 'Old Org',
            'capacity': 50,
            'event_type': 'Meetup',
            'registered_user_ids': [(6, 0, [])],
        })

    def test_valid_event_update_message(self):
        """Test processing a valid UPDATE event message"""

        update_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <UpdateEvent>
            <EventUUID>2025-05-20T14:30:45.123456Z</EventUUID>
            <EventName>Updated Event Name</EventName>
            <Description>New description</Description>
            <StartDateTime>2025-05-21T10:00:00.000000Z</StartDateTime>
            <EndDateTime>2025-05-21T18:00:00.000000Z</EndDateTime>
            <EventLocation>New Location</EventLocation>
            <Organisator>New Org</Organisator>
            <Capacity>100</Capacity>
            <EventType>Conference</EventType>
            <RegisteredUsers>
                <User><UUID>2025-01-01T12:00:00.000000Z</UUID></User>
            </RegisteredUsers>
        </UpdateEvent>
        '''

        # Call the consumer
        self.thread._process_message(update_xml.encode('utf-8'))

        # Refresh and check values
        self.event.flush()
        self.event.invalidate_cache()
        event = self.event

        self.assertEqual(event.name, 'Updated Event Name')
        self.assertEqual(event.description, 'New description')
        self.assertEqual(event.start_datetime, '2025-05-21T10:00:00.000000Z')
        self.assertEqual(event.end_datetime, '2025-05-21T18:00:00.000000Z')
        self.assertEqual(event.location, 'New Location')
        self.assertEqual(event.organisator, 'New Org')
        self.assertEqual(event.capacity, 100)
        self.assertEqual(event.event_type, 'Conference')
        self.assertEqual(event.registered_user_ids.ids, self.partner.ids)

    def test_update_event_not_found(self):
        """Test update with non-existent UUID"""
        bad_uuid_xml = '''<UpdateEvent>
            <EventUUID>2099-01-01T00:00:00.000000Z</EventUUID>
            <EventName>Doesn't Matter</EventName>
        </UpdateEvent>'''
        with self.assertRaises(ValueError) as context:
            self.thread._process_message(bad_uuid_xml.encode('utf-8'))
        self.assertIn("No event found with UUID", str(context.exception))

    def test_empty_registered_users(self):
        """Test clearing all registered users"""
        # Set one partner before clearing
        self.event.registered_user_ids = self.partner

        clear_users_xml = '''<UpdateEvent>
            <EventUUID>2025-05-20T14:30:45.123456Z</EventUUID>
            <RegisteredUsers></RegisteredUsers>
        </UpdateEvent>'''

        self.thread._process_message(clear_users_xml.encode('utf-8'))

        self.event.flush()
        self.event.invalidate_cache()
        self.assertEqual(self.event.registered_user_ids, self.env['res.partner'])

