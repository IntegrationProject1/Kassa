#!/bin/bash

export PGPASSWORD="odoo"

# 🔁 Start the heartbeat service in the background
python3 /opt/heartbeat_service.py &
echo "Heartbeat service started."

# Check if DB exists
if ! psql -U odoo -h db -lqt | grep -q "^   odoo   |"; then
    echo "Database 'odoo' does not exist. Creating and initializing..."
    createdb -U odoo -h db odoo
    odoo -d odoo -i point_of_sale,rabbitmq_heartbeat,user_delete,user_update,user_create,rabbitmq_orders \
         --without-demo=all --addons-path=/mnt/extra-addons -u all --stop-after-init
else
    echo "Database 'odoo' already exists. Skipping initialization."
    odoo -d odoo -u point_of_sale,rabbitmq_heartbeat,user_delete,user_update,user_create,rabbitmq_orders \
         --addons-path=/mnt/extra-addons --stop-after-init
fi

# 🟢 Finally, start Odoo server
exec odoo -c /etc/odoo/odoo.conf
