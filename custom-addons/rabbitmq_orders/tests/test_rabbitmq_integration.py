from odoo.tests.common import TransactionCase
from odoo import fields
import pika
import xml.etree.ElementTree as ET
import logging
import time

_logger = logging.getLogger(__name__)

class TestRabbitMQIntegration(TransactionCase):
    def setUp(self):
        super(TestRabbitMQIntegration, self).setUp()
        # Configuratie voor RabbitMQ (pas de host en queue aan indien nodig)
        self.rabbitmq_host = "rabbitmq"
        self.queue_name = "order.created"
        self.exchange = "billing"
        # Maak verbinding met RabbitMQ en zorg dat de exchange en queue bestaan
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitmq_host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type="topic", durable=True)
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        # Zorg dat de queue leeg is voor de test
        self.channel.queue_purge(queue=self.queue_name)

    def tearDown(self):
        # Maak de RabbitMQ queue weer leeg en sluit de verbinding
        self.channel.queue_purge(queue=self.queue_name)
        self.connection.close()
        super(TestRabbitMQIntegration, self).tearDown()

    def test_generate_xml_message(self):
        """Test dat de gegenereerde XML-data overeenkomt met de verwachte structuur en inhoud."""
        # Stel de verwachte waarden vast (deze waarden kun je aanpassen op basis van je testdata)
        expected_order_id = "59"
        expected_date = "03/26/2025 18:22:57"
        expected_user = "Administrator"
        expected_product = {
            "ProductName": "Aperol Spritz",
            "Quantity": "3.0",
            "UnitPrice": "9.0",
            "TotalPrice": "27.0",
        }
        expected_payment = {
            "PaymentMethod": "Cash",
            "Amount": "31.05",
        }
        expected_total_amount = "31.05"
        expected_taxes = "4.05"
        expected_total_paid = "31.05"

        # Simuleer een order-object met de benodigde attributen voor de test.
        # Dit kan je doen door een dummy-record te maken of door met mocks te werken.
        # Hieronder is een voorbeeld waarbij je een dummy order maakt (pas dit aan volgens je Odoo omgeving).
        dummy_order = type("DummyOrder", (object,), {})()
        dummy_order.id = int(expected_order_id)
        from datetime import datetime
        dummy_order.date_order = datetime.strptime(expected_date, "%m/%d/%Y %H:%M:%S")
        dummy_order.user_id = type("DummyUser", (object,), {"name": expected_user})()
        
        # Maak dummy data voor order lines
        dummy_product = type("DummyProduct", (object,), {"name": expected_product["ProductName"]})()
        dummy_line = type("DummyLine", (object,), {
            "product_id": dummy_product,
            "qty": float(expected_product["Quantity"]),
            "price_unit": float(expected_product["UnitPrice"]),
            "price_subtotal": float(expected_product["TotalPrice"])
        })()
        dummy_order.lines = [dummy_line]
        
        # Maak dummy data voor betalingen
        dummy_payment_method = type("DummyPaymentMethod", (object,), {"name": expected_payment["PaymentMethod"]})()
        dummy_payment = type("DummyPayment", (object,), {
            "payment_method_id": dummy_payment_method,
            "amount": float(expected_payment["Amount"])
        })()
        dummy_order.payment_ids = [dummy_payment]

        # Extra orderinformatie
        dummy_order.amount_total = float(expected_total_amount)
        dummy_order.amount_tax = float(expected_taxes)
        dummy_order.amount_paid = float(expected_total_paid)

        # Roep de generate_xml_message functie aan
        from odoo.addons.kassa.models.pos_order import PosOrder  # Pas aan naar jouw module naam indien nodig
        pos_order_obj = PosOrder()
        generated_xml = pos_order_obj.generate_xml_message(dummy_order)

        # Parse de gegenereerde XML
        root = ET.fromstring(generated_xml)

        # Valideer de Order informatie
        self.assertEqual(root.find("OrderID").text, expected_order_id, "OrderID komt niet overeen.")
        self.assertEqual(root.find("Date").text, expected_date, "Datum komt niet overeen.")
        self.assertEqual(root.find("User").text, expected_user, "User komt niet overeen.")

        # Valideer de Product informatie
        products = root.find("Products")
        self.assertIsNotNone(products, "Geen Products element gevonden.")
        product = products.find("Product")
        self.assertEqual(product.find("ProductName").text, expected_product["ProductName"], "ProductNaam komt niet overeen.")
        self.assertEqual(product.find("Quantity").text, expected_product["Quantity"], "Quantity komt niet overeen.")
        self.assertEqual(product.find("UnitPrice").text, expected_product["UnitPrice"], "UnitPrice komt niet overeen.")
        self.assertEqual(product.find("TotalPrice").text, expected_product["TotalPrice"], "TotalPrice komt niet overeen.")

        # Valideer de Payment informatie
        payments = root.find("Payments")
        self.assertIsNotNone(payments, "Geen Payments element gevonden.")
        payment = payments.find("Payment")
        self.assertEqual(payment.find("PaymentMethod").text, expected_payment["PaymentMethod"], "PaymentMethod komt niet overeen.")
        self.assertEqual(payment.find("Amount").text, expected_payment["Amount"], "Amount komt niet overeen.")

        # Valideer overige orderinformatie
        self.assertEqual(root.find("TotalAmount").text, expected_total_amount, "TotalAmount komt niet overeen.")
        self.assertEqual(root.find("Taxes").text, expected_taxes, "Taxes komt niet overeen.")
        self.assertEqual(root.find("TotalPaid").text, expected_total_paid, "TotalPaid komt niet overeen.")

if __name__ == "__main__":
    import unittest
    unittest.main()
