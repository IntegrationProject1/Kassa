{
    'name': 'POS Integration with RabbitMQ',
    'version': '1.0',
    'depends': ['point_of_sale'],
    'data': [
        'security/ir.model.access.csv',
        'data/cron_jobs.xml',
    ],

    'installable': True,
    'application': False,
}