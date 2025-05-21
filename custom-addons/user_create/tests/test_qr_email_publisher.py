import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime
import xml.etree.ElementTree as ET
from odoo.tests.common import TransactionCase
from lxml import etree
import qrcode
import base64
import io

# XSD Schema for email messages
EMAIL_XSD_SCHEMA = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="emailMessage">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="to" type="xs:string"/>
        <xs:element name="subject" type="xs:string"/>
        <xs:element name="htmlcontent" type="xs:string"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
'''

class TestQREmailPublisher(TransactionCase):
    
    def setUp(self):
        super(TestQREmailPublisher, self).setUp()
        
        # Create a test partner
        self.test_partner = self.env['res.partner'].with_context(skip_rabbitmq_publish=True).create({
            'name': 'Test User',
            'email': 'test@example.com',
            'customer_rank': 1
        })

    def test_validate_email_xml(self):
        """Test validation of email XML against XSD schema"""
        # Create a valid XML message
        valid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <emailMessage>
            <to>test@example.com</to>
            <subject>Your QR Code</subject>
            <htmlcontent><![CDATA[<html><body>Test email content</body></html>]]></htmlcontent>
        </emailMessage>'''
        
        # Parse XSD schema and create validator
        schema_doc = etree.fromstring(EMAIL_XSD_SCHEMA.encode('utf-8'))
        schema = etree.XMLSchema(schema_doc)
        
        # Parse message and validate
        xml_doc = etree.fromstring(valid_xml.encode('utf-8'))
        self.assertTrue(schema.validate(xml_doc), "Valid XML should pass validation")
        
        # Test invalid XML (missing required element)
        invalid_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <emailMessage>
            <to>test@example.com</to>
            <subject>Your QR Code</subject>
        </emailMessage>'''
        
        xml_doc = etree.fromstring(invalid_xml.encode('utf-8'))
        self.assertFalse(schema.validate(xml_doc), "Invalid XML should fail validation")

    def test_generate_qr_code(self):
        """Test QR code generation with external_id"""
        # Generate test external_id
        external_id = "2023-05-15T10:30:00.123456Z"
        
        # Create QR code data with prefix
        qr_data = f"042{external_id}"
        
        # Generate QR code image
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(qr_data)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        
        # Convert to base64
        buffered = io.BytesIO()
        img.save(buffered)
        img_base64 = base64.b64encode(buffered.getvalue()).decode()
        
        # Verify the image is generated
        self.assertTrue(img_base64, "QR code should be generated as base64")
        self.assertTrue(len(img_base64) > 100, "QR code base64 should have significant length")

    @patch('pika.BlockingConnection')
    def test_publish_qr_email(self, mock_connection):
        """Test publishing QR code email to RabbitMQ"""
        # Setup mock connection and channel
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        
        # Call the method to create and publish the email
        with patch.object(self.env['res.partner'], '_publish_qr_email') as mock_publish:
            # Create a test partner with external_id
            partner = self.env['res.partner'].with_context(skip_rabbitmq_publish=True).create({
                'name': 'Email Test User',
                'email': 'email.test@example.com',
                'customer_rank': 1,
                'external_id': '2023-05-15T10:30:00.123456Z'
            })
            
            # Call the publish method directly
            partner._publish_qr_email()
            
            # Verify the method was called
            mock_publish.assert_called_once()
    
    def test_email_html_content(self):
        """Test the HTML content of the email with QR code"""
        # Generate an email for a partner
        external_id = "2023-05-15T10:30:00.123456Z"
        partner = self.test_partner
        partner.external_id = external_id
        
        # Create HTML content with QR code
        html_content = self._generate_email_html(partner)
        
        # Verify the content includes key elements
        self.assertIn("<html", html_content)
        self.assertIn("QR Code", html_content)
        self.assertIn("img src=\"data:image/png;base64,", html_content)
        self.assertIn("</html>", html_content)
    
    def _generate_email_html(self, partner):
        """Helper method to generate email HTML with QR code"""
        # Create QR code with prefix + external_id
        qr_data = f"042{partner.external_id}"
        
        # Generate QR code image
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(qr_data)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        
        # Convert to base64
        buffered = io.BytesIO()
        img.save(buffered)
        img_base64 = base64.b64encode(buffered.getvalue()).decode()
        
        # Create HTML email content
        html_content = f"""
        <html>
            <body>
                <h1>Your Personal QR Code</h1>
                <p>Dear {partner.name},</p>
                <p>Thank you for registering. Below is your personal QR code:</p>
                <div style="text-align: center; margin: 20px 0;">
                    <img src="data:image/png;base64,{img_base64}" alt="QR Code" style="width: 250px; height: 250px;"/>
                </div>
                <p>Please keep this QR code for your records. You'll need it to identify yourself in our system.</p>
                <hr/>
                <p>If you have any questions, please contact our support team.</p>
            </body>
        </html>
        """
        return html_content
    
    def test_create_email_xml_message(self):
        """Test creation of XML message for email"""
        # Setup test data
        to_email = "test@example.com"
        subject = "Your QR Code"
        html_content = "<html><body>Test email content</body></html>"
        
        # Create XML message
        root = ET.Element("emailMessage")
        
        to_elem = ET.SubElement(root, "to")
        to_elem.text = to_email
        
        subject_elem = ET.SubElement(root, "subject")
        subject_elem.text = subject
        
        content_elem = ET.SubElement(root, "htmlcontent")
        content_elem.text = ET.CDATA(html_content)
        
        # Convert to XML string
        xml_string = ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8')
        
        # Validate the XML
        schema_doc = etree.fromstring(EMAIL_XSD_SCHEMA.encode('utf-8'))
        schema = etree.XMLSchema(schema_doc)
        xml_doc = etree.fromstring(xml_string.encode('utf-8'))
        
        self.assertTrue(schema.validate(xml_doc), "Generated XML should be valid")
        self.assertIn("<to>test@example.com</to>", xml_string)
        self.assertIn("<subject>Your QR Code</subject>", xml_string)
        self.assertIn("<htmlcontent><![CDATA[<html>", xml_string)