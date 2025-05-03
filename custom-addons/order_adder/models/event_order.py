# from odoo import models, fields

# class EventOrder(models.Model):
#     _name = 'event.order'
#     _description = 'Order Placed at an Event by a User'
#     _order = 'order_date desc'

#     order_date = fields.Char(required=True, string="Order Date")
#     uuid = fields.Char(required=True, string="UUID")  # external_id of the user
#     event_id = fields.Many2one(
#         'event.event',
#         string='Event',
#         required=True,
#         help='Event this order is associated with'
#     )
#     user_id = fields.Many2one(
#         'res.partner',
#         string='User',
#         required=True,
#         domain="[('external_id', '!=', False)]",
#         help='User who placed the order (linked by external_id)'
#     )
#     product_line_ids = fields.One2many(
#         'event.order.line',
#         'order_id',
#         string='Products'
#     )


# class EventOrderLine(models.Model):
#     _name = 'event.order.line'
#     _description = 'Order Line for Event Order'

#     order_id = fields.Many2one('event.order', string='Order', required=True, ondelete='cascade')
#     product_nr = fields.Float(string='Product Number', required=True)
#     quantity = fields.Float(string='Quantity', required=True)
#     unit_price = fields.Float(string='Unit Price', required=True)
