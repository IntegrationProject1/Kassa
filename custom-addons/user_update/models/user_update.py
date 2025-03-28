import pika
import os
import json
from odoo import models, api

class ResPartner(models.Model):
    _inherit = 'res.partner'

    def send_update_message(self, partner_data):
        """Send a RabbitMQ message with updated user information."""
        try:
            # Get RabbitMQ credentials from the environment variables
            rabbitmq_host = os.getenv('RABBITMQ_HOST')
            rabbitmq_user = os.getenv('RABBITMQ_USER')
            rabbitmq_password = os.getenv('RABBITMQ_PASSWORD')
            queue_name = 'kassa_user_update'

            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()

            try:
                # Check if the queue exists (passive=True)
                channel.queue_declare(queue=queue_name, passive=True)
                print(f"Queue '{queue_name}' already exists.")
            except pika.exceptions.ChannelClosedByBroker:
                # If the queue doesn't exist, create it
                channel = connection.channel()  # Reopen the channel
                channel.queue_declare(queue=queue_name)
                print(f"Queue '{queue_name}' created successfully.")

            # Convert the partner data to a JSON string
            message = json.dumps(partner_data)

            # Publish the message to the queue
            channel.basic_publish(exchange='', routing_key=queue_name, body=message)
            print(f"Message sent to queue '{queue_name}': {message}")

            # Close the connection
            connection.close()
        except Exception as e:
            print(f"Error sending RabbitMQ message: {e}")

    def write(self, vals):
        """Override the write method to send a RabbitMQ message on update."""
        # Call the original write method to update the user
        result = super(ResPartner, self).write(vals)

        # Prepare the updated partner data
        for partner in self:
            partner_data = {
                'id': partner.id,
                'name': partner.name,
                'mobile': partner.mobile,
                'email': partner.email,
                'street': partner.street,
                'city': partner.city,
                'country': partner.country_id.name if partner.country_id else None,
                'zip': partner.zip,
                'state': partner.state_id.name if partner.state_id else None,
                'phone': partner.phone,
                'website': partner.website,
            }

            # Send the RabbitMQ message
            self.send_update_message(partner_data)

        return result