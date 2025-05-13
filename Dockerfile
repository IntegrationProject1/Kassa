FROM odoo
USER root
RUN apt-get update && apt-get install -y python3-pika && apt-get install -y dotenv
RUN pip3 install --break-system-packages xmlschema
RUN pip3 install qrcode[pil]
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
COPY ./config/odoo.conf /etc/odoo/odoo.conf
COPY ./init.sh /
RUN chmod +x /init.sh 
USER odoo
CMD ["/init.sh"]