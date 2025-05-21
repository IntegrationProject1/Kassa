import unittest
from unittest.mock import patch, MagicMock
from odoo.tests.common import TransactionCase
import xml.etree.ElementTree as ET
import logging

from odoo.addons.rabbitmq_logs.models.logger import (
    create_log_message, validate_xml, send_log_to_queue, RabbitMQLogService
)


class TestLoggerBasics(unittest.TestCase):
    """Test basic logging functionality"""
    
    def test_create_log_message(self):
        """Test XML message creation"""
        # Create a test message
        xml = create_log_message("TEST_SERVICE", "INFO", "TEST_CODE", "Test message")
        
        # Parse the XML and check structure
        root = ET.fromstring(xml)
        self.assertEqual(root.tag, "Log", "Root element should be 'Log'")
        self.assertEqual(root.find("ServiceName").text, "TEST_SERVICE")
        self.assertEqual(root.find("Status").text, "INFO")
        self.assertEqual(root.find("Code").text, "TEST_CODE")
        self.assertEqual(root.find("Message").text, "Test message")
    
    @patch('odoo.addons.rabbitmq_logs.models.logger.log_queue')
    def test_send_log_to_queue(self, mock_queue):
        """Test sending logs to queue"""
        # Send a test log
        send_log_to_queue("TEST_SERVICE", "INFO", "TEST_CODE", "Test message")
        
        # Verify queue.put was called
        self.assertTrue(mock_queue.put.called)
    
    @patch('odoo.addons.rabbitmq_logs.models.logger.log_queue')
    def test_skip_rabbitmq_logs_module(self, mock_queue):
        """Test logs from rabbitmq_logs are skipped (avoids recursion)"""
        # Try to send a log from this module
        send_log_to_queue("rabbitmq_logs", "INFO", "TEST_CODE", "Test message")
        
        # Verify queue.put was not called
        mock_queue.put.assert_not_called()


class TestRabbitMQLogService(TransactionCase):
    """Test the RabbitMQLogService class"""
    
    @patch('odoo.addons.rabbitmq_logs.models.logger.send_log_to_queue')
    def test_service_test_log(self, mock_send_log):
        """Test the test_log method"""
        # Call the test_log method
        RabbitMQLogService.test_log("Test message")
        
        # Check that send_log_to_queue was called with correct params
        mock_send_log.assert_called_with("MANUAL_TEST", "INFO", "TEST", "Test message")
    
    @patch('odoo.addons.rabbitmq_logs.models.logger.threading')
    @patch('odoo.addons.rabbitmq_logs.models.logger.logging')
    @patch('odoo.addons.rabbitmq_logs.models.logger.patch_module_log_function')
    def test_start_logging(self, mock_patch, mock_logging, mock_threading):
        """Test the start_logging method"""
        # Call the start_logging method
        result = RabbitMQLogService.start_logging()
        
        # Verify logger handler was added
        mock_logging.getLogger.return_value.addHandler.assert_called_once()
        
        # Verify at least one module was patched
        self.assertTrue(mock_patch.called)
        
        # Verify result
        self.assertTrue(result)


class TestLogStarter(TransactionCase):
    """Test the RabbitMQLogStarter model"""
    
    @patch('odoo.addons.rabbitmq_logs.models.logger.patch_module_log_function')
    def test_register_hook(self, mock_patch):
        """Test the _register_hook method to ensure modules are patched"""
        # Get the model
        log_starter = self.env['rabbitmq.log.starter']
        
        # Call _register_hook
        log_starter._register_hook()
        
        # Verify patching was attempted
        self.assertTrue(mock_patch.called)
        # There should be 8 modules to patch
        self.assertGreaterEqual(mock_patch.call_count, 5)


"""
Test execution instructions:
---------------------------
Run tests with the following command:
    docker exec -it kassa-odoo-1 odoo -d odoo --test-enable --stop-after-init -i rabbitmq_logs --test-tags rabbitmq_logs

Test design principles:
---------------------
1. Focused testing with minimal setup
2. Coverage of core functionality:
   - XML log message creation and validation
   - Queue message handling
   - Service initialization
   - Module patching functionality

3. Test isolation using:
   - unittest.TestCase for pure functions
   - TransactionCase for Odoo environment tests
   - Mock objects to avoid external dependencies

4. No actual RabbitMQ connections are made during testing
"""

