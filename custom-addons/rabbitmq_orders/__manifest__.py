{
    'name': 'POS Integration with RabbitMQ',
    'version': '1.0',
    'depends': ['point_of_sale'],
    'data': [
        'data/cron_jobs.xml',
        'security/ir.model.access.csv',
    ],

    'installable': True,
    'application': False,
}