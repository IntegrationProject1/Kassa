FROM odoo
USER root
RUN apt-get update && apt-get install -y python3-pika
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
COPY ./config/odoo.conf /etc/odoo/odoo.conf
COPY ./init.sh /
RUN chmod +x /init.sh  # DIRECT NA DE COPY
USER odoo
CMD ["/init.sh"]