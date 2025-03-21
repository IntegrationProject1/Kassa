#!/bin/bash

export PGPASSWORD="odoo"



# Check if the database already exists
if ! psql -U odoo -h db -lqt | grep -q "^   odoo   |"; then
    echo "Database 'odoo' does not exist. Creating and initializing..."
    # Create the database
    createdb -U odoo -h db odoo

    # Install POS and your custom module
    odoo -d odoo -i point_of_sale,rabbitmq_heartbeat --without-demo=all --addons-path=/mnt/extra-addons -u all --stop-after-init
else
    echo "Database 'odoo' already exists. Skipping initialization."

    # Update the modules (just in case)
    odoo -d odoo -u point_of_sale,rabbitmq_heartbeat --addons-path=/mnt/extra-addons --stop-after-init
fi

# Start Odoo server
exec odoo -c /etc/odoo/odoo.conf