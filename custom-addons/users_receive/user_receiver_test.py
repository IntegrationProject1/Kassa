import pika

# RabbitMQ connection
RABBITMQ_HOST = "localhost"
USERNAME = "" 
PASSWORD = ""

credentials = pika.PlainCredentials(USERNAME, PASSWORD)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
channel = connection.channel()

# XML test to see if a user is created that the message is sent to the queue
xml_message = """<?xml version="1.0" encoding="UTF-8"?>
<UserMessage>
    <ActionType>create</ActionType>
    <UserID>12345</UserID>
    <TimeOfAction>2024-03-27T14:30:00</TimeOfAction>
    <Password>hashedpassword</Password>
    <FirstName>Jan</FirstName>
    <LastName>Jansen</LastName>
    <PhoneNumber>+31612345678</PhoneNumber>
    <EmailAddress>jan@example.com</EmailAddress>
    <Business>
        <BusinessName>Jansen BV</BusinessName>
        <BusinessEmail>info@jansenbv.nl</BusinessEmail>
        <RealAddress>Stationsstraat 10, Amsterdam</RealAddress>
        <BTWNumber>NL123456789B01</BTWNumber>
        <FacturationAddress>Postbus 100, Amsterdam</FacturationAddress>
    </Business>
</UserMessage>
"""

# Send the XML message to the queue
queue_name = "kassa_user_create" # queue name for user creation
channel.basic_publish(exchange='', routing_key=queue_name, body=xml_message)
print(f"✅ XML send to {queue_name}")

connection.close()

