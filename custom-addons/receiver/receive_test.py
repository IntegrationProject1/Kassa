import pika

RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'crm_user_create' # can be changed with the other queues

xml_message = """<UserMessage>
    <ActionType>CREATE</ActionType>
    <UserID>12345</UserID>
    <TimeOfAction>2025-03-24T12:34:56Z</TimeOfAction>
    <FirstName>John</FirstName>
    <LastName>Doe</LastName>
    <PhoneNumber>+1234567890</PhoneNumber>
    <EmailAddress>john.doe@example.com</EmailAddress>
    <Business>
        <BusinessName>Example Corp</BusinessName>
        <BusinessEmail>contact@example.com</BusinessEmail>
        <RealAddress>123 Business St, City, Country</RealAddress>
        <BTWNumber>BE0123456789</BTWNumber>
        <FacturationAddress>456 Invoice Ave, City, Country</FacturationAddress>
    </Business>
</UserMessage>"""

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
channel = connection.channel()

# Ensure the queue exists
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# Publish XML message to the queue
channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=xml_message)

print(f"XML test message sent to {QUEUE_NAME}")
connection.close()
