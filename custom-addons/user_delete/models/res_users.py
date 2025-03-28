from odoo import models, api

class ResUsers(models.Model):
    _inherit = 'res.users'
    
    def unlink(self):
        """Override unlink to publish message to RabbitMQ before deleting users"""
        publisher = self.env['user.delete.rabbitmq.publisher']
        for user in self:
            publisher.publish_user_delete(user.id)
        
        return super(ResUsers, self).unlink()