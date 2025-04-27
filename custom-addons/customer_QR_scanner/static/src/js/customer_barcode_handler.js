odoo.define('customer_QR_scanner.CustomerBarcodeHandler', function (require) {
    'use strict';

    const { PosGlobalState } = require('point_of_sale.models');
    const Registries = require('point_of_sale.Registries');

    const PREFIX = '@@';
    const SUFFIX = '##';

    const CustomPosGlobalState = (PosGlobalState) =>
        class extends PosGlobalState {
            async _processBarcode(barcode) {
                // Custom barcode handling for email
                if (barcode.startsWith(PREFIX) && barcode.endsWith(SUFFIX)) {
                    const cleanBarcode = barcode.slice(PREFIX.length, -SUFFIX.length);

                    // Search for customer by email
                    const customerByEmail = this.db.get_partner_by_email(cleanBarcode);
                    if (customerByEmail) {
                        this.get_order().set_client(customerByEmail);
                        this.chrome.showNotification(`Customer selected: ${customerByEmail.name}`);
                        return true;
                    } else {
                        this.chrome.showNotification('Customer not found by email.');
                        return false;
                    }
                }

                // Call the default barcode handling for products and other items
                return super._processBarcode(barcode);
            }
        };

    Registries.Model.extend(PosGlobalState, CustomPosGlobalState);
});
