import unittest
from odoo.tests.common import TransactionCase
from odoo.addons.event_rabbitmq_consumer.models.event_delete_consumer import EventDeleteThread


class TestEventDeleteConsumer(TransactionCase):

    def setUp(self):
        super().setUp()
        self.thread = EventDeleteThread(self.env)

        self.event = self.env['event.event'].create({
            'uuid': '2025-05-20T14:30:45.123456Z',
            'name': 'Event To Delete',
            'description': 'Should be removed',
            'start_datetime': '2025-05-21T09:00:00.000000Z',
            'end_datetime': '2025-05-21T17:00:00.000000Z',
            'location': 'Delete Hall',
            'organisator': 'Delete Org',
            'capacity': 10,
            'event_type': 'Test',
        })

    def test_valid_delete_message(self):
        """Test that a valid delete message removes the event"""
        delete_xml = '''<DeleteEvent>
            <UUID>2025-05-20T14:30:45.123456Z</UUID>
        </DeleteEvent>'''

        self.thread._process_message(delete_xml.encode('utf-8'))

        self.event.flush()
        self.assertFalse(self.env['event.event'].search([('uuid', '=', '2025-05-20T14:30:45.123456Z')]))

    def test_delete_nonexistent_event(self):
        """Test that deleting a non-existent event does not crash"""
        delete_xml = '''<DeleteEvent>
            <UUID>2099-01-01T00:00:00.000000Z</UUID>
        </DeleteEvent>'''

        # Should not raise error
        self.thread._process_message(delete_xml.encode('utf-8'))
        # Nothing should be found, nothing should happen — just log

    def test_invalid_xml(self):
        """Test that malformed XML raises error"""
        broken_xml = '''<DeleteEvent><UUID>bad</UUID'''
        with self.assertRaises(Exception):
            self.thread._process_message(broken_xml.encode('utf-8'))

    def test_invalid_schema(self):
        """Test that XML not matching the schema is rejected"""
        invalid_schema_xml = '''<DeleteEvent></DeleteEvent>'''
        with self.assertRaises(ValueError) as ctx:
            self.thread._process_message(invalid_schema_xml.encode('utf-8'))
        self.assertIn("Invalid XML structure", str(ctx.exception))
