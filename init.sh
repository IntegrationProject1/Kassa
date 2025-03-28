export PGPASSWORD="odoo"

if ! psql -U odoo -h db -lqt | grep -q "^   odoo   |"; then
    echo "Database 'odoo' does not exist. Creating and initializing..."
    createdb -U odoo -h db odoo
    odoo -d odoo -i point_of_sale,rabbitmq_heartbeat,user_delete,user_update --without-demo=all --addons-path=/mnt/extra-addons -u all --stop-after-init

else
    echo "Database 'odoo' already exists. Skipping initialization."
    odoo -d odoo -u point_of_sale,rabbitmq_heartbeat,user_delete,user_update --addons-path=/mnt/extra-addons --stop-after-init
fi

exec odoo -c /etc/odoo/odoo.conf