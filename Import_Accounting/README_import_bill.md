# Bill and Refund Import Script

This script allows you to import bills and refunds into Odoo from an Excel file with multiple sheets.

## Features

- **Mode Selection**: Choose to import bills, refunds, or both
- **Interactive Mode**: User-friendly menu for selecting import mode
- **Command Line Support**: Pass mode as command line argument
- **Dry Run Mode**: Test imports without actually creating records
- **Error Logging**: Detailed logging of import process
- **Progress Tracking**: Real-time progress display

## Usage

### Command Line Mode
```bash
# Import only bills
python import_bill.py bill

# Import only refunds
python import_bill.py refund

# Import both bills and refunds
python import_bill.py both

# Show help
python import_bill.py help
```

### Interactive Mode
```bash
# Run without arguments to see the interactive menu
python import_bill.py
```

## Configuration

Edit the following variables in the script:

- `DRY_RUN`: Set to `True` to test without importing
- `LOG_ERRORS`: Set to `False` to disable logging
- `SHOW_PROGRESS`: Set to `False` to disable progress display
- `data_file`: Path to your Excel file

## Excel File Structure

The Excel file should contain the following sheets:
- `Bill`: For bill records (required when importing bills)
- `Refund`: For refund records (required when importing refunds)

### Required Columns

Each sheet must contain these columns:
- `name`: Document number
- `partner_id`: Vendor name
- `invoice_date`: Document date
- `account_id`: Account code
- `quantity`: Quantity
- `price_unit`: Price per unit

### Optional Columns

- `date`: Accounting date
- `partner_code`: Vendor code
- `old_partner_code`: Old vendor code
- `journal`: Journal name
- `ref`: Reference document
- `label`: Line description
- `tax_ids`: Tax information
- `payment_reference`: Payment reference
- `note`: Notes/narration

## Log Files

Log files are created in the `logs` directory with names based on import mode:
- `bill_import_log_YYYYMMDD_HHMMSS.csv` - For bill imports
- `refund_import_log_YYYYMMDD_HHMMSS.csv` - For refund imports
- `bill_refund_import_log_YYYYMMDD_HHMMSS.csv` - For combined imports

## Examples

1. **Test Import (Dry Run)**
   - Set `DRY_RUN = True` in the script
   - Run the script to test without creating records

2. **Import Only Bills**
   ```bash
   python import_bill.py bill
   ```

3. **Import Both Bills and Refunds**
   ```bash
   python import_bill.py both
   ```

## Error Handling

The script includes comprehensive error handling:
- Validates required columns
- Checks data formats
- Handles connection errors
- Logs all errors for troubleshooting

## Notes

- The script automatically creates vendors if they don't exist
- Existing documents in draft state can be updated
- Documents in other states cannot be modified
- Account codes are validated against the chart of accounts