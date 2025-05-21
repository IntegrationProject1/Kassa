{
    'name': 'RabbitMQ Logging',
    'version': '1.0',
    'summary': 'Sends logs to RabbitMQ log_monitoring exchange',
    'description': '''
        This module captures logs from all Odoo modules and forwards them to RabbitMQ.
        It uses a standardized XML format for logs and sends them to the log_monitoring exchange.
    ''',
    'category': 'Tools',
    'author': 'Mathis de Brouwer',
    'depends': ['base'],
    'installable': True,
    'application': False,
}