# Vendor Name Update Script

## Overview

This script updates vendor names in Odoo 17 by matching partner codes from an Excel file with existing vendors in the system.

## Features

- Reads vendor data from Excel file
- Matches vendors by `partner_code` or `ref` field
- Updates vendor names when matches are found
- Supports dry-run mode for testing
- Comprehensive logging and error handling
- Progress tracking and summary reports

## Requirements

- Python 3.6+
- pandas library
- xmlrpc library (usually included with Python)

## Configuration

The script uses the following configuration (can be modified in the script):

```python
CONFIG = {
    'server_url': 'http://mogth.work:8069',
    'database': 'Test_import',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'data_file': 'Update Customer name/Contact_name_update.xlsx',
    'dry_run': False
}
```

## Excel File Format

The Excel file should contain the following columns:

1. **Partner Code (รหัส Partner_code)** - The partner code to match with Odoo
2. **Display Name (ชื่อ Vender)** - The new vendor name to update

The script is flexible with column names and will try to identify the correct columns based on their content.

## Usage

### Basic Usage

```bash
python "Update Customer name/update_vender_name.py"
```

### Dry Run Mode (Recommended First)

```bash
python "Update Customer name/update_vender_name.py" --dry-run
```

### Specify Different Excel File

```bash
python "Update Customer name/update_vender_name.py" --data-file "path/to/your/excel/file.xlsx"
```

### Command Line Arguments

- `--dry-run`: Run in dry-run mode (no changes made to Odoo)
- `--data-file`: Specify path to Excel data file

## How It Works

1. **Connection**: Establishes connection to Odoo using XML-RPC
2. **Reading Data**: Reads the Excel file and extracts partner codes and vendor names
3. **Matching**: Searches for vendors in Odoo using:
   - `partner_code` field first
   - `ref` field as fallback
   - Only matches records where `supplier_rank > 0` (vendors only)
4. **Updating**: Updates vendor names when matches are found
5. **Logging**: Creates detailed logs of all operations

## Output

### Console Output
- Real-time progress information
- Success/error messages for each record
- Final summary report

### Log File
- Detailed log saved to: `Update Customer name/vendor_update.log`
- Includes timestamps and detailed operation information

### Summary Report
```
==================================================
VENDOR NAME UPDATE SUMMARY
==================================================
Total records processed: 100
Successfully updated: 85
Not found in system: 10
Errors encountered: 5
==================================================
```

## Error Handling

The script handles various error conditions:
- Connection failures to Odoo
- Missing or corrupted Excel files
- Invalid data formats
- Vendors not found in system
- Permission issues during updates

## Security Notes

- Credentials are stored in the script configuration
- Consider using environment variables for production use
- Always test with dry-run mode first
- Ensure proper backup of Odoo database before running

## Troubleshooting

### Common Issues

1. **Authentication Failed**
   - Check server URL, database, username, and password
   - Verify user has proper permissions in Odoo

2. **Excel File Not Found**
   - Check file path and permissions
   - Ensure Excel file is not open in another program

3. **Column Not Found**
   - Verify Excel file contains required columns
   - Check column names match expected format

4. **No Vendors Found**
   - Verify partner codes exist in Odoo
   - Check if vendors have `supplier_rank > 0`

## Example Excel File Structure

| Display Name (ชื่อ Vender) | Partner Code (รหัส Partner_code) |
|---------------------------|----------------------------------|
| บริษัท ตัวอย่าง จำกัด | VENDOR001 |
| ห้างหุ้นส่วนจำกัด ทดสอบ | VENDOR002 |

## Support

For issues or questions:
1. Check the log file for detailed error messages
2. Verify Odoo connection and permissions
3. Test with a small dataset first
4. Use dry-run mode to validate data before actual updates