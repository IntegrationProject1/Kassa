import pika
import os
import json
import threading
from odoo import models, fields, api

class ResPartner(models.Model):
    _inherit = 'res.partner'

    @api.model
    def createQueue(self):
        """Ensure the RabbitMQ queue exists, and create it if it doesn't."""
        queue_name = 'user_create'
        try:
            # Get RabbitMQ credentials from the environment variables
            rabbitmq_host = os.getenv('RABBITMQ_HOST')
            rabbitmq_user = os.getenv('RABBITMQ_USER')
            rabbitmq_password = os.getenv('RABBITMQ_PASSWORD')
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)

            # Connect to RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()

            try:
                # Check if the queue exists (passive=True)
                channel.queue_declare(queue=queue_name, passive=True)
                print("Queue " + queue_name + " already exists.")
            except pika.exceptions.ChannelClosedByBroker:
                # If the queue doesn't exist, create it
                channel = connection.channel()  # Reopen the channel
                channel.queue_declare(queue=queue_name)
                print("Queue " + queue_name + " created successfully.")

            # Close the connection
            connection.close()
        except Exception as e:
            print("Connection or queue creation error: ", e)

    @api.model
    def create(self, vals):
        """Override the create method to send user data to RabbitMQ."""
        # Call the original create method to create the customer
        partner = super(ResPartner, self).create(vals)

        # Define the queue name
        queue_name = 'user_create'

        try:
            # Get RabbitMQ credentials from the environment variables
            rabbitmq_host = os.getenv('RABBITMQ_HOST')
            rabbitmq_user = os.getenv('RABBITMQ_USER')
            rabbitmq_password = os.getenv('RABBITMQ_PASSWORD')
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)

            # Connect to RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()

            # Ensure the queue exists
            self.createQueue()

            # Serialize the partner data into a dictionary
            partner_data = {
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

            # Convert the dictionary to a JSON string
            message = json.dumps(partner_data)

            # Publish the message to the queue
            channel.basic_publish(exchange='', routing_key=queue_name, body=message)

            # Close the connection
            connection.close()
        except Exception as e:
            print("Connection or message publish error: ", e)

        return partner