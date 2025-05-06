export PGPASSWORD="odoo"

if ! psql -U odoo -h db -lqt | grep -q "^   odoo   |"; then
    echo "Database 'odoo' does not exist. Creating and initializing..."
    createdb -U odoo -h db odoo

    # For the first installation
    odoo -d odoo -i point_of_sale,rabbitmq_heartbeat,user_delete,user_update,user_create,rabbitmq_orders,rabbitmq_logs,event_adder,event_consumer,order_adder --without-demo=all --addons-path=/mnt/extra-addons -u all --stop-after-init

else
    echo "Database 'odoo' already exists. Skipping initialization."
    # For the update
    odoo -d odoo -u point_of_sale,rabbitmq_heartbeat,user_delete,user_update,user_create,rabbitmq_orders,rabbitmq_logs,event_adder,event_consumer,order_adder --addons-path=/mnt/extra-addons --stop-after-init

fi

exec odoo -c /etc/odoo/odoo.conf