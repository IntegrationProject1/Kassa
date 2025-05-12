import datetime
import xml.etree.ElementTree as ET
from unittest.mock import patch, MagicMock, call
from odoo.tests import tagged, TransactionCase


class TestRabbitMQOrders(TransactionCase):
    
    def setUp(self):
        super(TestRabbitMQOrders, self).setUp()
        # Create test partner
        self.test_partner = self.env['res.partner'].create({
            'name': 'Test Customer',
            'external_id': '12345678-1234-1234-1234-123456789012',
        })
        
        # Create test products
        self.product_1 = self.env['product.product'].create({
            'name': 'Test Product 1',
            'barcode': 'TP0001',
            'lst_price': 10.0,
        })
        
        self.product_2 = self.env['product.product'].create({
            'name': 'Test Product 2',
            'barcode': 'TP0002',
            'lst_price': 15.0,
        })
        
        # Create test event
        self.test_event = self.env['event.event'].create({
            'name': 'Test Event',
            'uuid': '87654321-4321-4321-4321-210987654321',
            'start_datetime': '2023-01-01T10:00:00Z',
            'end_datetime': '2023-01-01T18:00:00Z',
            'registered_user_ids': [(4, self.test_partner.id)],
        })
        
        # Mock POS order
        self.pos_config = self.env['pos.config'].create({'name': 'Test POS'})
        self.pos_session = self.env['pos.session'].create({
            'user_id': self.env.uid,
            'config_id': self.pos_config.id,
        })
        
        # Create payment methods
        self.account_payment = MagicMock()
        self.account_payment.payment_method_id.name = 'Customer Account'
        self.account_payment.payment_method_id.is_cash_count = False
        self.account_payment.payment_method_id.use_payment_terminal = False
        
        self.cash_payment = MagicMock()
        self.cash_payment.payment_method_id.name = 'Cash'
        self.cash_payment.payment_method_id.is_cash_count = True
        self.cash_payment.payment_method_id.use_payment_terminal = False
        
    @patch('odoo.addons.rabbitmq_orders.models.pos_order.OrderRabbitMQPublisher._publish_message')
    def test_store_event_order(self, mock_publish):
        # Create a test POS order
        pos_order = self.env['pos.order'].create({
            'session_id': self.pos_session.id,
            'partner_id': self.test_partner.id,
            'lines': [(0, 0, {
                'product_id': self.product_1.id,
                'qty': 2,
                'price_unit': self.product_1.lst_price,
            })],
        })
        
        # Mock payment on account
        payment = MagicMock()
        payment.payment_method_id.name = 'Customer Account'
        pos_order.payment_ids = [payment]
        
        # Test the store event order function
        publisher = self.env['order.rabbitmq.publisher']
        publisher._store_event_order(pos_order, self.test_event)
        
        # Check if event order was created
        event_order = self.env['event.order'].search([
            ('event_id', '=', self.test_event.id),
            ('partner_id', '=', self.test_partner.id),
            ('origin_pos_order_id', '=', pos_order.id),
        ])
        self.assertTrue(event_order, "Event order should be created")
        self.assertEqual(len(event_order.order_line_ids), 1, "Order should have one product line")
        
    def test_event_invoice_action(self):
        # Test the manual invoice action
        with patch('odoo.addons.rabbitmq_orders.models.pos_order.OrderRabbitMQPublisher._process_event_billing') as mock_process:
            result = self.test_event.action_send_invoices()
            
            # Verify process_event_billing was called
            mock_process.assert_called_once_with(self.test_event)
            
            # Check if event is marked as invoiced
            self.assertTrue(self.test_event.is_invoiced, "Event should be marked as invoiced")
            
            # Verify notification is returned
            self.assertEqual(result['type'], 'ir.actions.client')
            self.assertEqual(result['tag'], 'display_notification')
            
    @patch('odoo.addons.rabbitmq_orders.models.pos_order.OrderRabbitMQPublisher.validate_xml_against_xsd')
    @patch('odoo.addons.rabbitmq_orders.models.pos_order.OrderRabbitMQPublisher._publish_message')
    def test_publish_order_to_queue(self, mock_publish, mock_validate):
        # Create a test POS order
        pos_order = self.env['pos.order'].create({
            'session_id': self.pos_session.id,
            'partner_id': self.test_partner.id,
            'lines': [(0, 0, {
                'product_id': self.product_1.id,
                'qty': 2,
                'price_unit': self.product_1.lst_price,
            })],
        })
        
        # Set up mocks
        mock_validate.return_value = True
        
        # Execute function
        publisher = self.env['order.rabbitmq.publisher']
        publisher._publish_order_to_queue(pos_order)
        
        # Check that validation was called
        mock_validate.assert_called_once()
        
        # Check that publish was called with correct queue
        mock_publish.assert_called_once()
        self.assertEqual(mock_publish.call_args[0][1], "order.created")
        
        # Verify that XML contains expected data
        xml_str = mock_publish.call_args[0][0]
        root = ET.fromstring(xml_str)
        self.assertEqual(root.tag, "Order")
        self.assertEqual(root.find("UUID").text, self.test_partner.external_id)
        
        products = root.find("Products")
        self.assertEqual(len(products.findall("Product")), 1)
        product = products.find("Product")
        self.assertEqual(product.find("ProductNR").text, str(self.product_1.id))
        self.assertEqual(product.find("Quantity").text, "2.00")
        self.assertEqual(product.find("UnitPrice").text, "10.00")
        
    @patch('odoo.addons.rabbitmq_orders.models.pos_order.OrderRabbitMQPublisher._publish_order_to_queue')
    @patch('odoo.addons.rabbitmq_orders.models.pos_order.OrderRabbitMQPublisher._store_event_order')
    def test_handle_order_during_active_event(self, mock_store, mock_publish):
        # Create a test POS order
        pos_order = self.env['pos.order'].create({
            'session_id': self.pos_session.id,
            'partner_id': self.test_partner.id,
            'lines': [(0, 0, {
                'product_id': self.product_1.id,
                'qty': 1,
                'price_unit': self.product_1.lst_price,
            })],
        })
        
        # Set payment to account payment type
        pos_order.payment_ids = [self.account_payment]
        
        # Set event to be active now
        now = datetime.datetime.now(datetime.timezone.utc)
        start = now - datetime.timedelta(hours=1)
        end = now + datetime.timedelta(hours=1)
        
        self.test_event.write({
            'start_datetime': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'end_datetime': end.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'is_invoiced': False
        })
        
        # Register the partner for the event
        self.test_event.write({
            'registered_user_ids': [(4, self.test_partner.id)]
        })
        
        # Test handling order
        with patch('odoo.addons.rabbitmq_orders.models.pos_order.datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = now
            mock_datetime.timezone.utc = datetime.timezone.utc
            
            publisher = self.env['order.rabbitmq.publisher']
            publisher._handle_order(pos_order)
            
            # Should store order with active event
            mock_store.assert_called_once_with(pos_order, self.test_event)
            mock_publish.assert_not_called()
            
    @patch('odoo.addons.rabbitmq_orders.models.pos_order.OrderRabbitMQPublisher._send_user_event_summary')
    def test_process_event_billing(self, mock_send_summary):
        # Create test event order
        pos_order = self.env['pos.order'].create({
            'session_id': self.pos_session.id,
            'partner_id': self.test_partner.id,
            'lines': [(0, 0, {
                'product_id': self.product_1.id,
                'qty': 2,
                'price_unit': self.product_1.lst_price,
            })],
        })
        
        # Mock payment on account
        pos_order.payment_ids = [self.account_payment]
        
        # Create an event order
        event_order = self.env['event.order'].create({
            'event_id': self.test_event.id,
            'partner_id': self.test_partner.id,
            'order_date': fields.Datetime.now(),
            'origin_pos_order_id': pos_order.id,
        })
        
        self.env['event.order.product'].create({
            'event_order_id': event_order.id,
            'product_nr': str(self.product_1.id),
            'quantity': 2.0,
            'unit_price': 10.0,
        })
        
        # Test the event billing process
        with patch('odoo.addons.rabbitmq_orders.models.pos_order.PosOrder.search') as mock_search:
            mock_search.return_value = pos_order
            
            publisher = self.env['order.rabbitmq.publisher']
            publisher._process_event_billing(self.test_event)
            
            # Check that user summary was called with correct data
            mock_send_summary.assert_called_once()
            self.assertEqual(mock_send_summary.call_args[0][0], self.test_event)
            self.assertEqual(mock_send_summary.call_args[0][1], self.test_partner.external_id)
            
            # Check that the product data was correctly processed
            products = mock_send_summary.call_args[0][2]
            self.assertIn(str(self.product_1.id), products)
            self.assertEqual(products[str(self.product_1.id)]['quantity'], 2.0)
            self.assertEqual(products[str(self.product_1.id)]['unit_price'], 10.0)
            
    def test_handle_order_with_cash_payment(self):
        # Create a test POS order with cash payment
        pos_order = self.env['pos.order'].create({
            'session_id': self.pos_session.id,
            'partner_id': self.test_partner.id,
            'lines': [(0, 0, {
                'product_id': self.product_1.id,
                'qty': 1,
                'price_unit': self.product_1.lst_price,
            })],
        })
        
        # Set payment to cash type
        pos_order.payment_ids = [self.cash_payment]
        
        # Test handling order - should not process for billing
        with patch('odoo.addons.rabbitmq_orders.models.pos_order.OrderRabbitMQPublisher._publish_order_to_queue') as mock_publish:
            with patch('odoo.addons.rabbitmq_orders.models.pos_order.OrderRabbitMQPublisher._store_event_order') as mock_store:
                publisher = self.env['order.rabbitmq.publisher']
                publisher._handle_order(pos_order)
                
                # Neither store nor publish should be called for cash payments
                mock_store.assert_not_called()
                mock_publish.assert_not_called()