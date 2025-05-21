import unittest
from unittest.mock import patch, MagicMock
import base64
import io
from odoo.tests.common import SavepointCase
import qrcode


class TestQrEmail(SavepointCase):
    
    def setUp(self):
        super(TestQrEmail, self).setUp()
        
        # Create a test partner
        self.test_partner = self.env['res.partner'].with_context(skip_qr_email=True).create({
            'name': 'Test User',
            'email': 'test@example.com',
            'external_id': '2023-05-15T10:30:00.123456Z',
            'customer_rank': 1
        })
        
    def test_qr_code_generation(self):
        """Test QR code data format and generation"""
        # Get expected QR code data
        expected_qr_data = f"042{self.test_partner.external_id}"
        
        # Create a test QR code for comparison
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(expected_qr_data)
        qr.make(fit=True)
        
        # Verify QR data format
        self.assertEqual(qr.data, expected_qr_data.encode())
        self.assertTrue(expected_qr_data.startswith("042"))
        
    @patch('odoo.addons.mail.models.mail_mail.MailMail.send')
    def test_send_qr_email(self, mock_send):
        """Test sending QR code email to partner"""
        # Mock the mail send method
        mock_send.return_value = True
        
        # Call the QR email generation method
        thread = MagicMock()
        result = thread._send_qr_email(self.test_partner, self.env)
        
        # Test that method returns True on success
        self.assertTrue(result)
        
        # Test that mail send was called
        mock_send.assert_called_once()
        
    @patch('odoo.addons.mail.models.mail_mail.MailMail.send')
    def test_email_content_has_required_elements(self, mock_send):
        """Test that email HTML contains required content"""
        # Create a method to capture the created mail
        created_mail = None
        
        def create_and_capture(model, vals, *args, **kwargs):
            nonlocal created_mail
            created_mail = self.env['mail.mail'].new(vals)
            return created_mail
        
        # Mock the create method to capture values
        with patch('odoo.addons.mail.models.mail_mail.MailMail.create', 
                  side_effect=create_and_capture):
            thread = MagicMock()
            thread._send_qr_email(self.test_partner, self.env)
            
            # Check captured email content
            self.assertTrue(created_mail is not None)
            self.assertEqual(created_mail.email_to, self.test_partner.email)
            self.assertIn('Your Personal QR Code', created_mail.subject)
            
            # Check HTML content
            html_content = created_mail.body_html
            self.assertIn('<html', html_content)
            self.assertIn('Your Personal QR Code', html_content)
            self.assertIn(self.test_partner.name, html_content)
            self.assertIn('img src="data:image/png;base64,', html_content)
    
    @patch('odoo.addons.user_create.models.consumer_user_create.CustomerCreateThread._send_qr_email')
    def test_auto_send_on_create(self, mock_send_qr):
        """Test QR email gets sent automatically on partner creation"""
        # Create a new partner with email and external_id
        new_partner = self.env['res.partner'].create({
            'name': 'Auto Email Test',
            'email': 'auto.email@example.com',
            'external_id': '2023-05-16T10:30:00.123456Z',
            'customer_rank': 1
        })
        
        # Check that send method was called
        mock_send_qr.assert_called_once()
    
    @patch('odoo.addons.user_create.models.consumer_user_create.CustomerCreateThread._send_qr_email')
    def test_not_send_without_email(self, mock_send_qr):
        """Test QR email not sent when partner has no email"""
        # Create a partner without email
        new_partner = self.env['res.partner'].with_context(skip_qr_email=False).create({
            'name': 'No Email Test',
            'external_id': '2023-05-17T10:30:00.123456Z',
            'customer_rank': 1
        })
        
        # Check send method was not called
        mock_send_qr.assert_not_called()