# Invoice and Credit Note Import Script for Odoo 17

## Overview

This script imports customer invoices and credit notes into Odoo 17 from an Excel template file. It supports importing both invoices and credit notes, with options to process them separately or together.

## Features

- **Dry Run Mode**: Test imports without actually creating data in Odoo
- **Error Logging**: Detailed logging of import errors for troubleshooting
- **Real-time Progress**: Live progress display during import process
- **Flexible Import Modes**: Import invoices only, credit notes only, or both
- **Customer Management**: Automatically finds or creates customers using partner codes
- **Document Updates**: Updates existing draft documents or creates new ones

## Prerequisites

- Python 3.x with required packages: pandas, xmlrpc, openpyxl
- Odoo 17 instance with XML-RPC access enabled
- Excel template file with proper format

## Installation

1. Ensure Python dependencies are installed:
```bash
pip install pandas openpyxl
```

2. Place the script in your project directory
3. Update connection settings in the script if needed

## Configuration

Update these variables in the script:

```python
# Connection Settings
url = 'http://your-odoo-server:8069'
db = 'your_database'
username = 'your_username'
password = 'your_password'

# Data file path
data_file = r'C:\path\to\your\Template_Invoice_Credit_note.xlsx'

# Import behavior
DRY_RUN = False  # Set to True for testing
LOG_ERRORS = True  # Enable error logging
SHOW_PROGRESS = True  # Show progress during import
```

## Excel Template Format

The Excel file should contain two sheets: `Invoice` and `Credit_Note`

### Required Columns

| Column | Description | Example |
|---------|-------------|----------|
| name | Document Number | INV-001 |
| invoice_date | Document Date | 2024-12-01 |
| partner_code | Customer Code | CUST001 |
| old_partner_code | Old Customer Code | OLD001 |
| journal | Journal Name | Sales Journal |
| partner_id | Customer Name | ABC Company |
| ref | Reference | PO-123 |
| label | Line Description | Product Sales |
| account_id | Account Code | 400001 |
| quantity | Quantity | 10 |
| price_unit | Unit Price | 100.50 |
| tax_ids | Tax IDs | VAT7 |
| payment_reference | Payment Reference | PAY-001 |
| note | Notes | Special terms |

## Usage

### Command Line Options

```bash
# Import only invoices
python import_invoice.py invoice

# Import only credit notes
python import_invoice.py credit_note

# Import both invoices and credit notes
python import_invoice.py both

# Interactive mode (will prompt for selection)
python import_invoice.py

# Show help
python import_invoice.py help
```

### Interactive Mode

When running without arguments, the script will prompt you to select an import mode:

1. Import Invoices only (from 'Invoice' sheet)
2. Import Credit Notes only (from 'Credit_Note' sheet)
3. Import both Invoices and Credit Notes (from both sheets)

## Import Process

1. **Data Validation**: Script validates required fields and data formats
2. **Customer Lookup**: Searches for existing customers by:
   - Partner code (partner_code)
   - Old partner code (old_partner_code)
   - Customer name (partner_id)
3. **Document Processing**: For each document:
   - Groups lines by document number
   - Checks if document already exists in Odoo
   - Creates new document or updates existing draft
   - Adds all invoice lines to the document
4. **Logging**: Records all operations and errors to log files

## Odoo Integration

### Document Types
- **Invoices**: Created as `out_invoice` move type
- **Credit Notes**: Created as `out_refund` move type

### Customer Creation
New customers are created with:
- `customer_rank: 1` (marked as customer)
- `company_type: 'company'` (default company type)
- Partner codes if provided

### Journal Selection
- Default: Sales journal (type='sale')
- Custom: Searches by journal name from Excel data

## Error Handling

### Common Issues and Solutions

1. **Connection Errors**
   - Verify Odoo server URL and credentials
   - Check XML-RPC is enabled in Odoo

2. **Missing Customers**
   - Script automatically creates new customers
   - Check partner codes in Excel file

3. **Account Not Found**
   - Verify account codes exist in Odoo
   - Check account code format (numbers only)

4. **Date Format Issues**
   - Script handles multiple date formats
   - Preferred format: YYYY-MM-DD

### Log Files

Log files are created in the `logs` directory with timestamps:
- `invoice_import_log_YYYYMMDD_HHMMSS.csv`
- `credit_note_import_log_YYYYMMDD_HHMMSS.csv`
- `invoice_credit_note_import_log_YYYYMMDD_HHMMSS.csv`

Log files contain:
- Timestamp
- Document Type
- Document Number
- Customer Name
- Status (Success/Error)
- Error Message
- Row Number

## Best Practices

1. **Test First**: Always run with `DRY_RUN = True` first
2. **Backup Data**: Backup your Odoo database before importing
3. **Validate Data**: Check Excel file for required columns and valid data
4. **Monitor Logs**: Review log files for any errors or warnings
5. **Batch Processing**: Import in reasonable batches to avoid timeouts

## Troubleshooting

### Script Won't Start
- Check Python version and dependencies
- Verify file permissions
- Ensure Excel template exists at specified path

### Import Fails Partway
- Check Odoo server connection stability
- Review error logs for specific issues
- Try smaller batches of data

### Customers Not Found
- Verify partner codes match Odoo exactly
- Check for extra spaces or special characters
- Ensure customer names match existing records

## Support

For issues or questions:
1. Check the log files for detailed error messages
2. Verify all prerequisites are met
3. Test with a small dataset first
4. Review this documentation for configuration options

## Version History

- **v1.0**: Initial release for Odoo 17
  - Support for invoice and credit note imports
  - Customer management with partner codes
  - Dry run mode and error logging
  - Real-time progress display