import pika
import xmlrpc.client
import json

# Odoo configuration
ODOO_URL = 'http://localhost:8069'
ODOO_DB = 'odoo'  
ODOO_USER = 'admin'  
ODOO_PASSWORD = 'admin'  

# RabbitMQ configuration
RABBITMQ_HOST = 'localhost'
QUEUES = [
    'crm_user_create',
    'facturatie_user_create',
    'frontend_user_create',
    'kassa_user_create',
    'user.create'
]

# make connection with odoo
def connect_to_odoo():
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    return models, uid

# make user in odoo
def create_user_in_odoo(data):
    models, uid = connect_to_odoo()

    user_data = {
        'name': data.get('name'),
        'email': data.get('email'),
        'login': data.get('email'),  # Login wordt standaard email
    }

    user_id = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.users', 'create', [user_data])
    print(f"User created in Odoo with ID: {user_id}")

# Callback function for rabbitmq messages
def callback(ch, method, properties, body):
    print(f"received message from {method.routing_key}: {body}")

    try:
        data = json.loads(body.decode('utf-8'))
        create_user_in_odoo(data)
    except Exception as e:
        print(f"error with compiling the message: {e}")

# Verbinding maken met RabbitMQ
def start_rabbitmq_listener():
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()

    for queue in QUEUES:
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)

    print("RabbitMQ listener started... waiting for messages.")
    channel.start_consuming()

if __name__ == "__main__":
    start_rabbitmq_listener()

