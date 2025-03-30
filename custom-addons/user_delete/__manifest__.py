{
    'name': 'User Delete via RabbitMQ',
    'version': '1.0',
    'summary': 'Delete users based on RabbitMQ messages',
    'description': '''
        This module listens to a RabbitMQ queue (kassa_user_delete) for XML messages
        with user deletion instructions. When a message is received, it looks up the user
        and deletes it from Odoo.
    ''',
    'category': 'Tools',
    'author': 'jente',
    'depends': ['base'],
    'data': [
        'security/ir.model.access.csv',
    ],
    'demo': [],
    'installable': True,
    'application': False,
    'auto_install': False,
    'license': 'LGPL-3',
}