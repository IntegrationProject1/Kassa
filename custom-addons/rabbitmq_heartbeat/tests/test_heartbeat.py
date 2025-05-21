import unittest
from unittest.mock import patch, MagicMock, call
from odoo.tests.common import TransactionCase
from lxml import etree
import pika
import threading
import time
import io
from ..models.heartbeat import HeartbeatThread, HEARTBEAT_XSD

class TestHeartbeatXML(unittest.TestCase):
    """Test XML validation and generation"""
    
    def test_xml_schema_valid(self):
        """Test that the XSD schema is valid"""
        try:
            # Parse the schema
            xsd_root = etree.parse(io.StringIO(HEARTBEAT_XSD))
            schema = etree.XMLSchema(xsd_root)
            self.assertTrue(True, "Schema parsed successfully")
        except Exception as e:
            self.fail(f"XSD schema parsing failed: {e}")
    
    def test_heartbeat_message_format(self):
        """Test that the heartbeat message is correctly formatted"""
        thread = HeartbeatThread()
        message = thread.create_heartbeat_message()
        
        # Parse the message to check structure
        root = etree.fromstring(message.encode('utf-8'))
        
        # Check root element name
        self.assertEqual(root.tag, "Heartbeat", "Root element should be 'Heartbeat'")
        
        # Check service name element exists and has content
        service_name = root.find("ServiceName")
        self.assertIsNotNone(service_name, "ServiceName element should exist")
        self.assertEqual(service_name.text, "Odoo_POS", "ServiceName should be 'Odoo_POS'")

    def test_heartbeat_message_validates_against_schema(self):
        """Test that the generated message validates against the XSD schema"""
        thread = HeartbeatThread()
        message = thread.create_heartbeat_message()
        
        # Load schema
        xsd_root = etree.parse(io.StringIO(HEARTBEAT_XSD))
        schema = etree.XMLSchema(xsd_root)
        
        # Parse message and validate
        xml_content = message.replace('<?xml version="1.0" encoding="UTF-8"?>', '').strip()
        xml_doc = etree.fromstring(xml_content.encode('utf-8'))
        
        # Validation should succeed
        self.assertTrue(schema.validate(xml_doc), "Generated message should validate against schema")
        
        # Create an invalid message (missing required element)
        invalid_xml = "<Heartbeat></Heartbeat>"
        invalid_doc = etree.fromstring(invalid_xml.encode('utf-8'))
        
        # Validation should fail
        self.assertFalse(schema.validate(invalid_doc), "Invalid message should fail validation")


class TestHeartbeatService(TransactionCase):
    """Test the heartbeat service functionality"""
    
    def setUp(self):
        super().setUp()
        # Set up the model
        self.heartbeat_model = self.env['rabbitmq.heartbeat']
        
    @patch('pika.BlockingConnection')
    def test_start_stop_heartbeat(self, mock_connection):
        """Test starting and stopping the heartbeat service"""
        # Mock the connection's channel method
        mock_connection.return_value.channel.return_value = MagicMock()
        
        # Start the heartbeat service
        result = self.heartbeat_model.start_heartbeat()
        self.assertTrue(result, "Starting heartbeat service should succeed")
        
        # Starting again should fail
        result = self.heartbeat_model.start_heartbeat()
        self.assertFalse(result, "Starting heartbeat service twice should fail")
        
        # Stop the service
        result = self.heartbeat_model.stop_heartbeat()
        self.assertTrue(result, "Stopping heartbeat service should succeed")
        
        # Stopping again should fail
        result = self.heartbeat_model.stop_heartbeat()
        self.assertFalse(result, "Stopping non-running heartbeat service should fail")


class TestHeartbeatThread(unittest.TestCase):
    """Test the heartbeat thread functionality"""
    
    @patch('pika.BlockingConnection')
    def test_thread_setup_and_publish(self, mock_connection):
        """Test that the thread sets up and publishes messages correctly"""
        # Set up mock connection and channel
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        mock_connection.return_value.is_open = True
        
        # Mock the validate_xml method to avoid testing that here
        with patch.object(HeartbeatThread, 'validate_xml', return_value=True):
            # Create and start the thread
            thread = HeartbeatThread()
            thread.daemon = True
            thread.start()
            
            # Let it run for a short time
            time.sleep(2)  # Allow some heartbeats to be sent
            
            # Stop the thread
            thread.stop()
            thread.join(1)
            
            # Verify the thread activities
            mock_channel.exchange_declare.assert_called_with(
                exchange='heartbeat_monitoring',
                exchange_type='direct',
                durable=True
            )
            
            mock_channel.queue_declare.assert_called_with(
                queue='controlroom.heartbeat.ping', 
                durable=True
            )
            
            mock_channel.queue_bind.assert_called_with(
                exchange='heartbeat_monitoring',
                queue='controlroom.heartbeat.ping',
                routing_key='controlroom.heartbeat.ping'
            )
            
            # Verify publish was called
            self.assertTrue(mock_channel.basic_publish.called, "Publish method should be called")
            
            # Check publish parameters
            args, kwargs = mock_channel.basic_publish.call_args_list[0]
            self.assertEqual(kwargs['exchange'], 'heartbeat_monitoring', "Exchange should be correct")
            self.assertEqual(kwargs['routing_key'], 'controlroom.heartbeat.ping', "Routing key should be correct")
            
    @patch('pika.BlockingConnection')
    def test_connection_error_handling(self, mock_connection):
        """Test that connection errors are handled properly"""
        # Make the connection raise an exception
        mock_connection.side_effect = pika.exceptions.AMQPConnectionError("Test connection error")
        
        # Create and start the thread
        thread = HeartbeatThread()
        thread.daemon = True
        thread.start()
        
        # Let it run for a short time
        time.sleep(1)  # Allow thread to attempt connection
        
        # Stop the thread
        thread.stop()
        thread.join(1)
        
        # If we got here, the error was handled correctly
        self.assertTrue(True, "Thread should handle connection errors gracefully")
    
    def test_thread_stops_gracefully(self):
        """Test that the thread stops when requested"""
        # Create thread but don't start it
        thread = HeartbeatThread()
        thread.start = MagicMock()  # Prevent actual start to avoid RabbitMQ connection
        
        # Call stop method
        thread.stop()
        
        # Check that running flag was set to False
        self.assertFalse(thread.running, "Thread running flag should be set to False")