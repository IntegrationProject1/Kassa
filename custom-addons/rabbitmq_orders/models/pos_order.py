from odoo import models, fields, api
import datetime
from datetime import timedelta
import xml.etree.ElementTree as ET
import pika
import os
import logging
from lxml import etree

_logger = logging.getLogger(__name__)

def log_message(message):
    print(f"[ORDER_MODULE] {message}")
    _logger.info(message)

ORDER_MESSAGE_XSD = '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="Order">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="Date" type="xs:dateTime"/>
        <xs:element name="UUID" type="xs:dateTime"/>
        <xs:element name="Products">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="Product" maxOccurs="unbounded">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="ProductNR" type="xs:decimal"/>
                    <xs:element name="Quantity" type="xs:decimal"/>
                    <xs:element name="UnitPrice" type="xs:decimal"/>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>'''

class Event(models.Model):
    _name = 'event.event'
    _description = 'External Event'
    _order = 'start_datetime desc'

    uuid = fields.Char(required=True, string="UUID", index=True)
    name = fields.Char(required=True)
    description = fields.Text()
    start_datetime = fields.Char(required=True)
    end_datetime = fields.Char(required=True)
    location = fields.Char()
    organisator = fields.Char()
    capacity = fields.Integer()
    event_type = fields.Char()

    registered_user_ids = fields.Many2many(
        'res.partner',
        'event_event_res_partner_rel',
        'event_event_id',
        'res_partner_id',
        string='Registered Users',
        help='Users registered for this event (linked by external_id)',
    )

    is_invoiced = fields.Boolean(string='Is Invoiced', default=False, 
                                 help='Indicates if this event has been invoiced')

    def action_send_invoices(self):
        """
        Handmatig facturen versturen naar facturatie service - alleen voor orders op rekening
        """
        self.ensure_one()
        log_message(f"=== Manual invoice action triggered for event {self.name} ===")
        
        if self.is_invoiced:
            log_message(f"Event {self.name} is already invoiced, showing warning")
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Al gefactureerd',
                    'message': 'Dit event is al gefactureerd.',
                    'type': 'warning',
                }
            }
        
        log_message(f"Delegating billing to RabbitMQ publisher for event {self.name}")
        # Delegeer facturatie naar RabbitMQ publisher
        self.env['order.rabbitmq.publisher']._process_event_billing(self)
        
        # Markeer event als gefactureerd
        log_message(f"Marking event {self.name} as invoiced")
        self.write({'is_invoiced': True})
        
        log_message(f"Invoice action completed for event {self.name}")
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Facturatie succesvol',
                'message': 'Alleen orders op rekening zijn doorgestuurd naar de facturatieservice.',
                'type': 'success',
            }
        }


class EventOrder(models.Model):
    _name = 'event.order'
    _description = 'Order linked to an Event and User'

    event_id = fields.Many2one('event.event', required=True)
    partner_id = fields.Many2one('res.partner', required=True)
    order_date = fields.Datetime(string='Order Date', default=fields.Datetime.now)
    order_line_ids = fields.One2many('event.order.product', 'event_order_id', string='Order Lines')
    origin_pos_order_id = fields.Many2one('pos.order', string='Origin POS Order')


class EventOrderProduct(models.Model):
    _name = 'event.order.product'
    _description = 'Product in an Event Order'

    event_order_id = fields.Many2one('event.order', required=True, ondelete='cascade')
    product_nr = fields.Char(required=True)
    quantity = fields.Float(required=True)
    unit_price = fields.Float(required=True)


class PosOrder(models.Model):
    _inherit = 'pos.order'

    @api.model
    def create(self, vals):
        order = super().create(vals)
        log_message(f"Order {order.id} created, checking for event linkage.")
        self.env['order.rabbitmq.publisher']._handle_order(order)
        return order


class OrderRabbitMQPublisher(models.AbstractModel):
    _name = 'order.rabbitmq.publisher'
    _description = 'RabbitMQ Publisher for Order Events'

    def _get_rabbitmq_connection_params(self):
        credentials = pika.PlainCredentials(
            os.environ.get('RABBITMQ_USER'),
            os.environ.get('RABBITMQ_PASSWORD')
        )
        return pika.ConnectionParameters(
            host=os.environ.get('RABBITMQ_HOST', 'rabbitmq'),
            port=int(os.environ.get('RABBITMQ_PORT', 5672)),
            credentials=credentials
        )

    def validate_xml_against_xsd(self, xml_string):
        try:
            log_message(f"Validating XML against XSD schema")
            xml_doc = etree.fromstring(xml_string.encode('utf-8'))
            xsd_doc = etree.fromstring(ORDER_MESSAGE_XSD.encode('utf-8'))
            schema = etree.XMLSchema(xsd_doc)
            is_valid = schema.validate(xml_doc)
            
            if is_valid:
                log_message(f"XML validation successful")
            else:
                validation_errors = schema.error_log
                log_message(f"XML validation failed with errors: {validation_errors}")
                
            return is_valid
        except Exception as e:
            log_message(f"XML validation error: {str(e)}")
            return False

    def _publish_message(self, message, queue_name):
        try:
            connection = pika.BlockingConnection(self._get_rabbitmq_connection_params())
            channel = connection.channel()

            routing_key = queue_name
            channel.exchange_declare(exchange='billing', exchange_type='topic', durable=True)
            channel.queue_declare(queue=queue_name, durable=True)
            channel.queue_bind(exchange='billing', queue=queue_name, routing_key=routing_key)

            channel.basic_publish(
                exchange='billing',
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/xml'
                )
            )
            log_message(f"Published order to {queue_name}")
            connection.close()
        except Exception as e:
            log_message(f"Error publishing order message: {e}")

    def _handle_order(self, order):
        # Check betaalmethode - alleen orders op rekening doorsturen
        log_message(f"=== Handling order {order.id} for partner {order.partner_id.name} ===")
        
        is_account_payment = False
        payment_methods = []
        
        # Check of een van de betalingen een "customer account" type is
        for payment in order.payment_ids:
            payment_name = payment.payment_method_id.name
            payment_methods.append(payment_name)
            log_message(f"Payment method found: {payment_name} (is_cash={payment.payment_method_id.is_cash_count}, uses_terminal={payment.payment_method_id.use_payment_terminal})")
            
            # In Odoo POS is 'account' meestal de betaalmethode voor klantrekeningen
            if payment.payment_method_id.use_payment_terminal == False and \
               payment.payment_method_id.is_cash_count == False:
                is_account_payment = True
                log_message(f"Found account payment method: {payment_name}")
        
        log_message(f"Order {order.id} payment methods: {', '.join(payment_methods)}")
        
        # Als het geen account payment is, sla deze over voor facturatie
        if not is_account_payment:
            log_message(f"Order {order.id} is paid with cash/card, skipping invoice processing")
            return
        
        log_message(f"Order {order.id} is paid on account, processing for invoicing")
        
        now = datetime.datetime.utcnow()
        partner = order.partner_id
        log_message(f"Checking if partner {partner.name} is registered for any active events")

        event = self.env['event.event'].search([
            ('start_datetime', '<=', now.strftime('%Y-%m-%d %H:%M:%S')),
            ('end_datetime', '>=', now.strftime('%Y-%m-%d %H:%M:%S')),
            ('registered_user_ids', 'in', partner.id)
        ], limit=1)

        if event:
            log_message(f"Found active event '{event.name}' for user '{partner.name}', will store for bulk processing")
            self._store_event_order(order, event)
        else:
            log_message(f"No active event found for user '{partner.name}', sending directly to queue")
            self._publish_order_to_queue(order)

    def _store_event_order(self, order, event):
        event_order = self.env['event.order'].create({
            'event_id': event.id,
            'partner_id': order.partner_id.id,
            'order_date': fields.Datetime.now(),
            'origin_pos_order_id': order.id,  # Referentie naar originele POS-order
        })
        for line in order.lines:
            self.env['event.order.product'].create({
                'event_order_id': event_order.id,
                'product_nr': str(line.product_id.id),
                'quantity': line.qty,
                'unit_price': line.price_unit,
            })
        log_message(f"Order {order.id} stored in event '{event.name}'")

    def _publish_order_to_queue(self, order):
        partner = order.partner_id
        uuid_value = partner.external_id

        if not uuid_value:
            log_message(f"Missing external_id for partner {partner.name}, skipping publish")
            return

        root = ET.Element("Order")
        ET.SubElement(root, "Date").text = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        ET.SubElement(root, "UUID").text = uuid_value

        products = ET.SubElement(root, "Products")
        for line in order.lines:
            product = ET.SubElement(products, "Product")
            ET.SubElement(product, "ProductNR").text = str(line.product_id.id)
            ET.SubElement(product, "Quantity").text = f"{line.qty:.2f}"
            ET.SubElement(product, "UnitPrice").text = f"{line.price_unit:.2f}"

        xml_str = ET.tostring(root, encoding='unicode')

        if not self.validate_xml_against_xsd(xml_str):
            log_message("Generated order XML failed XSD validation")
            return

        self._publish_message(xml_str, queue_name="order.created")

    @api.model
    def send_event_summary_to_billing(self, event_id=None):
        """
        Verzamel alle aankopen per gebruiker voor een afgelopen event en stuur naar facturatie.
        Alleen events die recent zijn afgelopen worden verwerkt, tenzij een specifiek event_id is opgegeven.
        """
        try:
            log_message(f"====================================================")
            log_message(f"CRON JOB: Checking for recently ended events to bill")
            log_message(f"Current time: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
            log_message(f"Specific event_id requested: {event_id or 'No'}")
            
            # Toon de laatste 3 events om debugging mogelijk te maken
            recent_events = self.env['event.event'].search([], order='id desc', limit=3)
            log_message(f"Most recent events in system (for reference):")
            for event in recent_events:
                log_message(f"  - Event: {event.name} (ID: {event.id})")
                log_message(f"  - Start: {event.start_datetime}")
                log_message(f"  - End: {event.end_datetime}")
                log_message(f"  - Invoiced: {event.is_invoiced}")
            
            # Zoek events die zojuist zijn afgelopen
            log_message(f"Starting to process events...")
            now = datetime.datetime.now()
            log_message(f"Now (local): {now}")
            one_hour_ago = now - timedelta(hours=1)
            log_message(f"One hour ago: {one_hour_ago}")
            
            # Haal niet-gefactureerde events op
            domain = [('is_invoiced', '=', False)]
            
            # Als een specifiek event_id is opgegeven, gebruik alleen dat event
            if event_id:
                domain.append(('id', '=', event_id))
                log_message(f"Filtering for specific event ID: {event_id}")
            else:
                # Anders, filter events die recent zijn afgelopen
                # Probeer parse te doen op end_datetime, vang fouten op voor verschillende formaten
                candidates = self.env['event.event'].search(domain)
                ended_events = []
                
                for event in candidates:
                    log_message(f"Checking if event {event.name} (ID: {event.id}) has ended")
                    try:
                        # Probeer ISO format (met T separator)
                        if 'T' in event.end_datetime:
                            end_dt_str = event.end_datetime.replace('Z', '+00:00')
                            end_dt = datetime.datetime.fromisoformat(end_dt_str)
                            log_message(f"  - End time parsed as ISO: {end_dt}")
                        else:
                            # Probeer standaard Odoo format
                            end_dt = datetime.datetime.strptime(event.end_datetime, '%Y-%m-%d %H:%M:%S')
                            log_message(f"  - End time parsed as standard: {end_dt}")
                        
                        # Check of event is afgelopen (eindtijd < nu)
                        if end_dt < now:
                            log_message(f"  - Event has ended, adding to processing list")
                            ended_events.append(event.id)
                        else:
                            log_message(f"  - Event has not ended yet, skipping")
                    except Exception as e:
                        log_message(f"  - Error parsing end date: {str(e)}, skipping event")
                
                if ended_events:
                    domain.append(('id', 'in', ended_events))
                    log_message(f"Filtering for ended events: {ended_events}")
                else:
                    log_message(f"No ended events found")
            
            log_message(f"Final search domain: {domain}")
            
            # Voer zoekopdracht uit met volledige filtering
            all_events = self.env['event.event'].search(domain)
            log_message(f"Found {len(all_events)} non-invoiced ended events to process")
            
            # Verwerk alle geselecteerde events
            processed_count = 0
            for event in all_events:
                log_message(f"Processing event: {event.name} (ID: {event.id})")
                self._process_event_billing(event)
                event.write({'is_invoiced': True})
                log_message(f"Successfully processed event {event.name}")
                processed_count += 1
            
            if processed_count == 0:
                log_message(f"No events were processed")
            else:
                log_message(f"Successfully processed {processed_count} events")
            
            log_message(f"CRON JOB: End of events check")
            log_message(f"====================================================")
            return True
        
        except Exception as e:
            # Log de fout om te zien waar de code vastloopt
            log_message(f"ERROR in send_event_summary_to_billing: {str(e)}")
            import traceback
            log_message(traceback.format_exc())
            return False
    
    def _process_event_billing(self, event):
        """Verwerk de facturatie voor één event - alleen voor rekening-orders"""
        log_message(f"========================================")
        log_message(f"Processing end-of-event billing for event: {event.name} (UUID: {event.uuid})")
        log_message(f"Event period: {event.start_datetime} to {event.end_datetime}")
        log_message(f"Registered users: {len(event.registered_user_ids)}")
        
        # Controleer of event al is gefactureerd
        if event.is_invoiced:
            log_message(f"Event {event.name} already invoiced, skipping")
            return
        
        # Verzamel alle orders voor dit event
        event_orders = self.env['event.order'].search([('event_id', '=', event.id)])
        log_message(f"Found {len(event_orders)} total orders for event {event.name}")
        
        if not event_orders:
            log_message(f"No orders found for event {event.name}, skipping billing")
            return
        
        # Groepeer orders per gebruiker - check alleen account payments
        user_orders = {}
        for registered_user in event.registered_user_ids:
            log_message(f"------------------------------------------")
            log_message(f"Processing user: {registered_user.name} (ID: {registered_user.id})")
            
            if not registered_user.external_id:
                log_message(f"User {registered_user.name} has no external_id, skipping")
                continue
            else:
                log_message(f"User {registered_user.name} external_id: {registered_user.external_id}")
                    
            # Verzamel alle order lines voor deze gebruiker tijdens dit event
            user_event_orders = event_orders.filtered(lambda o: o.partner_id.id == registered_user.id)
            log_message(f"Found {len(user_event_orders)} event orders for user {registered_user.name}")
            
            if not user_event_orders:
                log_message(f"No orders for user {registered_user.name} in event {event.name}")
                continue
            
            # Filter orders om alleen rekening-orders te selecteren
            account_orders = []
            for user_order in user_event_orders:
                log_message(f"Checking origin POS order for event order {user_order.id}")
                # Get the original POS order
                pos_order = self.env['pos.order'].search([
                    ('id', '=', user_order.origin_pos_order_id.id)
                ], limit=1)
                
                if not pos_order:
                    log_message(f"No original POS order found for event order {user_order.id}, skipping")
                    continue
                
                log_message(f"Found original POS order: {pos_order.id}, checking payment methods")
                    
                # Check payment method
                is_account = False
                for payment in pos_order.payment_ids:
                    payment_name = payment.payment_method_id.name
                    log_message(f"Payment method: {payment_name} (is_cash={payment.payment_method_id.is_cash_count}, uses_terminal={payment.payment_method_id.use_payment_terminal})")
                    
                    if payment.payment_method_id.use_payment_terminal == False and \
                       payment.payment_method_id.is_cash_count == False:
                        is_account = True
                        log_message(f"Found account payment method: {payment_name} for order {pos_order.id}")
                        break
                            
                if is_account:
                    log_message(f"Order {pos_order.id} is on account, adding to billing")
                    account_orders.append(user_order)
                else:
                    log_message(f"Order {pos_order.id} is NOT on account, skipping")
            
            log_message(f"Found {len(account_orders)} account-based orders for user {registered_user.name}")
            
            if not account_orders:
                log_message(f"No account-based orders for user {registered_user.name}, skipping")
                continue
                    
            # Voor elke gebruiker, verzamel producten met totale hoeveelheden en prijzen
            product_summary = {}
            for order in account_orders:
                log_message(f"Processing order {order.id} with {len(order.order_line_ids)} line items")
                for line in order.order_line_ids:
                    product_nr = line.product_nr
                    if product_nr not in product_summary:
                        product_summary[product_nr] = {
                            'quantity': 0,
                            'unit_price': line.unit_price  # Neem de laatste prijs
                        }
                        log_message(f"Added new product {product_nr} to summary")
                    
                    product_summary[product_nr]['quantity'] += line.quantity
                    log_message(f"Updated product {product_nr}: quantity={product_summary[product_nr]['quantity']}, unit_price={product_summary[product_nr]['unit_price']}")
            
            # Alleen doorgaan als er producten zijn voor deze gebruiker
            if product_summary:
                log_message(f"Product summary for user {registered_user.name}: {len(product_summary)} products")
                user_orders[registered_user.external_id] = product_summary
            else:
                log_message(f"No products found for user {registered_user.name}, skipping")
        
        # Maak en verstuur een bericht voor elke gebruiker
        log_message(f"------------------------------------------")
        log_message(f"Processing billing for {len(user_orders)} users with account orders")
        
        for user_uuid, products in user_orders.items():
            log_message(f"Sending billing for user {user_uuid} with {len(products)} products")
            self._send_user_event_summary(event, user_uuid, products)
        
        log_message(f"Completed billing processing for event {event.name}")
        log_message(f"========================================")
    
    def _send_user_event_summary(self, event, user_uuid, products):
        """Maak en verstuur een samenvattingsbericht voor één gebruiker"""
        log_message(f"Creating billing XML for user {user_uuid} in event {event.name}")
        
        root = ET.Element("Order")
        ET.SubElement(root, "Date").text = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        ET.SubElement(root, "UUID").text = user_uuid
        ET.SubElement(root, "EventUUID").text = event.uuid
        
        products_element = ET.SubElement(root, "Products")
        for product_nr, details in products.items():
            product = ET.SubElement(products_element, "Product")
            ET.SubElement(product, "ProductNR").text = product_nr
            ET.SubElement(product, "Quantity").text = f"{details['quantity']:.2f}"
            ET.SubElement(product, "UnitPrice").text = f"{details['unit_price']:.2f}"
            log_message(f"Added product {product_nr}: quantity={details['quantity']}, unit_price={details['unit_price']}")
        
        xml_str = ET.tostring(root, encoding='unicode')
        log_message(f"Generated XML:\n{xml_str}")
        
        if not self.validate_xml_against_xsd(xml_str):
            log_message(f"Generated event summary XML for user {user_uuid} failed XSD validation")
            return
        
        log_message(f"Sending event summary for user {user_uuid} in event {event.name} to queue")
        self._publish_message(xml_str, queue_name="facturatie.order.event")
