import pika
import os
import json
import threading
from odoo import models, fields, api

class ResPartner(models.Model):
    _inherit = 'res.partner'

    @api.model
    def createQueue(self):
        """Ensure the RabbitMQ exchange and queue exist, and bind them."""
        exchange_name = 'kassa'
        queue_name = 'kassa_user_create'
        try:
            # Get RabbitMQ credentials from the environment variables
            rabbitmq_host = os.getenv('RABBITMQ_HOST')
            rabbitmq_user = os.getenv('RABBITMQ_USER')
            rabbitmq_password = os.getenv('RABBITMQ_PASSWORD')
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)

            # Connect to RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()

            # Check if the exchange exists, create if it doesn't
            try:
                channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True, passive=True)
                print(f"Exchange '{exchange_name}' already exists.")
            except pika.exceptions.ChannelClosedByBroker:
                print(f"Exchange '{exchange_name}' does not exist. Creating it.")
                channel = connection.channel()  # Reopen the channel
                channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

            # Check if the queue exists, create if it doesn't
            try:
                channel.queue_declare(queue=queue_name, durable=True, passive=True)
                print(f"Queue '{queue_name}' already exists.")
            except pika.exceptions.ChannelClosedByBroker:
                print(f"Queue '{queue_name}' does not exist. Creating it.")
                channel = connection.channel()  # Reopen the channel
                channel.queue_declare(queue=queue_name, durable=True)

            # Bind the queue to the exchange
            channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=queue_name)

            print(f"Exchange '{exchange_name}' and queue '{queue_name}' are set up successfully.")

            # Close the connection
            connection.close()
        except Exception as e:
            print("Connection or queue creation error: ", e)

    @api.model
    def create(self, vals):
        """Override the create method to send user data to RabbitMQ."""
        # Call the original create method to create the customer
        partner = super(ResPartner, self).create(vals)

        # Define the exchange and queue name
        exchange_name = 'kassa'
        queue_name = 'kassa_user_create'

        try:
            # Get RabbitMQ credentials from the environment variables
            rabbitmq_host = os.getenv('RABBITMQ_HOST')
            rabbitmq_user = os.getenv('RABBITMQ_USER')
            rabbitmq_password = os.getenv('RABBITMQ_PASSWORD')
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)

            # Connect to RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()

            # Ensure the exchange and queue exist
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

            # Publish the message to the exchange
            channel.basic_publish(exchange=exchange_name, routing_key=queue_name, body=message)

            # Close the connection
            connection.close()
        except Exception as e:
            print("Connection or message publish error: ", e)

        return partner