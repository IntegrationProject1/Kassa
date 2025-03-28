import pika
import xmlrpc.client
import xml.etree.ElementTree as ET

# Odoo Configuration
ODOO_URL = 'http://localhost:8069'
ODOO_DB = 'odoo'  
ODOO_USER = 'admin'  
ODOO_PASSWORD = 'admin'  

# RabbitMQ Configuration
RABBITMQ_HOST = 'localhost'
QUEUES = [
    'crm_user_create',
    'facturatie_user_create',
    'frontend_user_create',
    'kassa_user_create',
    'user.create'
]

# Connect to Odoo
def connect_to_odoo():
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    return models, uid

# Parse XML message into a dictionary
def parse_xml_message(xml_string):
    root = ET.fromstring(xml_string)

    user_data = {
        'action_type': root.findtext('ActionType'),
        'user_id': root.findtext('UserID'),
        'timestamp': root.findtext('TimeOfAction'),
        'first_name': root.findtext('FirstName'),
        'last_name': root.findtext('LastName'),
        'phone': root.findtext('PhoneNumber'),
        'email': root.findtext('EmailAddress'),
        'business_name': root.findtext('Business/BusinessName'),
        'business_email': root.findtext('Business/BusinessEmail'),
        'real_address': root.findtext('Business/RealAddress'),
        'btw_number': root.findtext('Business/BTWNumber'),
        'facturation_address': root.findtext('Business/FacturationAddress')
    }

    return user_data

# Create a user in Odoo
def create_user_in_odoo(data):
    if data['action_type'] != 'CREATE':
        print(f"Ignored action: {data['action_type']}")
        return

    models, uid = connect_to_odoo()

    user_data = {
        'name': f"{data['first_name']} {data['last_name']}",
        'email': data['email'],
        'login': data['email'],
        'phone': data['phone'],
    }

    user_id = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.users', 'create', [user_data])
    print(f"User created in Odoo with ID: {user_id}")

# RabbitMQ message callback function
def callback(ch, method, properties, body):
    print(f"Received message from {method.routing_key}: {body}")

    try:
        data = parse_xml_message(body.decode('utf-8'))
        create_user_in_odoo(data)
    except Exception as e:
        print(f"Error processing message: {e}")

# Start RabbitMQ listener
def start_rabbitmq_listener():
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()

    for queue in QUEUES:
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)

    print("RabbitMQ listener started... Waiting for messages.")
    channel.start_consuming()

if __name__ == "__main__":
    start_rabbitmq_listener()
