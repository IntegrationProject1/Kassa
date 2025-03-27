import pika
import xml.etree.ElementTree as ET

# RabbitMQ connection
RABBITMQ_HOST = "localhost"
USERNAME = ""
PASSWORD = ""

credentials = pika.PlainCredentials(USERNAME, PASSWORD)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
channel = connection.channel() 

# List from all queues of user create & update
queues = [
    "crm_user_create", "crm_user_update",
    "facturatie_user_create", "facturatie_user_update",
    "frontend_user_create", "frontend_user_update",
    "kassa_user_create", "kassa_user_update"
]

# Declare all queues
for queue in queues:
    channel.queue_declare(queue=queue, durable=True)

def parse_xml_message(xml_message):
    """try to get all the information from the XML message"""
    try:
        root = ET.fromstring(xml_message)
        action_type = root.find("ActionType").text
        user_id = root.find("UserID").text
        time_of_action = root.find("TimeOfAction").text
        email = root.find("EmailAddress").text if root.find("EmailAddress") is not None else "unknown"

        business = root.find("Business")
        business_name = business.find("BusinessName").text if business is not None and business.find("BusinessName") is not None else "no company"

        return {
            "ActionType": action_type,
            "UserID": user_id,
            "TimeOfAction": time_of_action,
            "Email": email,
            "BusinessName": business_name
        }
    except Exception as e:
        print(f"❌ error with XML-processing: {e}")
        return None

def callback(ch, method, properties, body):
    """ Callback-function that receives and processes XML-messages """
    xml_message = body.decode()
    user_data = parse_xml_message(xml_message)
    
    # print the message
    if user_data:
        print(f"📥 getting message from {method.routing_key}:")
        print(f"   🔹 action: {user_data['ActionType']}")
        print(f"   🔹 User ID: {user_data['UserID']}")
        print(f"   🔹 time: {user_data['TimeOfAction']}")
        print(f"   🔹 E-mail: {user_data['Email']}")
        print(f"   🔹 company: {user_data['BusinessName']}\n")
    else:
        print(f"⚠️ Invalid XML-message received: {xml_message}")

    ch.basic_ack(delivery_tag=method.delivery_tag)  # acknowledge the message

# listen to all queues
for queue in queues:
    channel.basic_consume(queue=queue, on_message_callback=callback)

# print message
print("Listening to the user create & update from all services...")
channel.start_consuming()
