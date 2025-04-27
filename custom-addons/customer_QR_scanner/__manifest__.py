{
    'name': 'Customer QR Scanner',
    'version': '1.0',
    'category': 'Point of Sale',
    'summary': 'Scan customer QR codes in PoS',
    'depends': ['base', 'point_of_sale'],
    'data': [
        'views/assets.xml',  # Your assets XML file to include JS
    ],
    'installable': True,
    'application': True,
}

