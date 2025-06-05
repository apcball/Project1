import xmlrpc.client
import pandas as pd
import io
import base64
from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HOST = 'http://mogdev.work:8069'
DB = 'MOG_LIVE3'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(HOST))
uid = common.authenticate(DB, USERNAME, PASSWORD, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(HOST))

@app.route('/api/inventory/backdate', methods=['POST'])
def update_inventory_backdate():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    try:
        # Read the Excel file
        file_content = file.read()
        df = pd.read_excel(io.BytesIO(file_content))

        # Ensure the required columns are present
        required_columns = ['name', 'date']
        if not all(column in df.columns for column in required_columns):
            return jsonify({"error": "Missing required columns in the Excel file"}), 400

        # Update inventory records
        updates = []
        for index, row in df.iterrows():
            document_name = row['name']
            effective_date = row['date']

            # Find the inventory record by document name
            inventory_ids = models.execute_kw(DB, uid, PASSWORD, 'stock.inventory', 'search', [[['name', '=', document_name]]])
            if inventory_ids:
                updates.append([inventory_ids, {'date': effective_date}])
            else:
                return jsonify({"error": f"Inventory with name {document_name} not found"}), 404

        # Batch update to reduce the number of API calls
        for inventory_ids, values in updates:
            models.execute_kw(DB, uid, PASSWORD, 'stock.inventory', 'write', [inventory_ids, values])

        return jsonify({"message": "Inventory dates updated successfully"}), 200

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)