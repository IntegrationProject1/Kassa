# from odoo import models, fields

# class Event(models.Model):
#     _name = 'event.event'
#     _description = 'External Event'
#     _order = 'start_datetime desc'

#     uuid = fields.Char(required=True, string="UUID", index=True)
#     name = fields.Char(required=True)
#     description = fields.Text()
#     start_datetime = fields.Char(required=True)
#     end_datetime = fields.Char(required=True)
#     location = fields.Char()
#     organisator = fields.Char()
#     capacity = fields.Integer()
#     event_type = fields.Char()
#     registered_user_ids = fields.Many2many(
#         'res.partner',
#         'event_event_res_partner_rel',
#         'event_event_id',
#         'res_partner_id',
#         string='Registered Users',
#         help='Users registered for this event (linked by external_id)',
#     )
