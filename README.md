# Odoo 17 Internal Transfer Import API

This script provides an API for importing internal transfer data from an Excel file into Odoo 17's inventory module.

## Features

- Connects to Odoo 17 using XML-RPC
- Reads internal transfer data from an Excel file
- Groups products by source and destination locations to create efficient transfers
- Creates stock pickings and stock moves in Odoo
- Confirms transfers automatically
- Provides detailed logging

## Requirements

- Python 3.6+
- Required Python packages:
  - xmlrpc.client (standard library)
  - pandas
  - openpyxl

## Installation

1. Clone this repository or download the script
2. Install required packages:

```bash
pip install pandas openpyxl
```

## Configuration

Edit the script to update the following configuration parameters:

```python
# Odoo connection parameters
ODOO_CONFIG = {
    'url': 'http://your-odoo-server:8069',
    'db': 'your_database',
    'username': 'your_username',
    'password': 'your_password'
}

# Excel file path
EXCEL_FILE = 'path/to/your/excel_file.xlsx'
```

## Excel File Format

The Excel file should contain the following columns:

- **Source Location**: The name of the source location
- **Destination Location**: The name of the destination location
- **Product Code**: The internal reference (default_code) of the product
- **Quantity**: The quantity to transfer
- **Reference** (optional): A reference for the transfer
- **Notes** (optional): Additional notes for the transfer

## Usage

Run the script using Python:

```bash
python fifo_internal_transfer_import.py
```

## How It Works

1. The script connects to the Odoo server using XML-RPC
2. It reads the Excel file containing transfer data
3. It groups products by source and destination locations
4. For each group, it creates a stock picking (internal transfer)
5. It adds stock moves for each product in the group
6. It confirms the transfers

## Logging

The script logs detailed information to both the console and a log file (`internal_transfer_import.log`). This includes:

- Connection status
- Excel file reading status
- Transfer creation details
- Errors and warnings
- Summary statistics

## Error Handling

The script includes comprehensive error handling:

- Connection errors
- Excel file reading errors
- Product or location not found errors
- Transfer creation errors

Errors are logged with detailed information to help troubleshoot issues.

## License

This project is licensed under the MIT License - see the LICENSE file for details.