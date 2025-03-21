FROM odoo

USER root

# Install dependencies if needed
RUN apt-get update && apt-get install -y python3-pika
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy the configuration file
COPY ./config/odoo.conf /etc/odoo/odoo.conf

# Add a script to initialize the database and install modules
COPY ./init.sh /
RUN chmod +x /init.sh  # DIRECT NA DE COPY

# Switch back to the odoo user
USER odoo

CMD ["/init.sh"]