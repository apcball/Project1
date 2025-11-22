# Partner Verification Script

This script verifies partner codes from an Excel file against the Odoo database by searching for them in the `old_code_partner` field of the `res.partner` model.

## Features

- **Batch Processing**: Processes partner codes in configurable batch sizes to avoid overwhelming the server
- **Robust Connection Handling**: Implements retry logic with exponential backoff for network issues
- **Progress Tracking**: Saves checkpoints to allow resuming interrupted processes
- **Comprehensive Logging**: Logs all operations to both file and console with timestamps
- **Error Recovery**: Handles connection errors and API failures gracefully
- **Output Reports**: Generates Excel files with partners not found in Odoo
- **Command Line Interface**: Flexible configuration through command line arguments

## Requirements

- Python 3.6+
- pandas
- xmlrpc.client (built-in)
- Required Excel file: `Partner_Find.xlsx` with partner codes in the first column

## Usage

### Basic Usage

```bash
python3 varify_partner.py
```

### With Custom Options

```bash
python3 varify_partner.py --batch-size 50 --output-dir ./results --log-dir ./logs
```

### Dry Run (No API Calls)

```bash
python3 varify_partner.py --dry-run
```

### Resume from Last Checkpoint

```bash
python3 varify_partner.py --resume
```

## Command Line Options

- `--data-file`: Path to Excel file containing partner codes (default: `Varify_partner/Partner_Find.xlsx`)
- `--output-dir`: Directory to save output files (default: `Varify_partner/output`)
- `--log-dir`: Directory to save log files (default: `Varify_partner/logs`)
- `--batch-size`: Number of records to process in each batch (default: 100)
- `--dry-run`: Run without making actual API calls
- `--resume`: Resume from last checkpoint
- `--help`: Show help message

## Output Files

### Log Files
- Located in `Varify_partner/logs/`
- Named with timestamp: `partner_verify_YYYYMMDD_HHMMSS.log`
- Contains detailed logs of all operations

### Output Files
- Located in `Varify_partner/output/`
- `partners_not_found_YYYYMMDD_HHMMSS.xlsx`: List of partners not found in Odoo
- `verification_summary_YYYYMMDD_HHMMSS.txt`: Summary statistics

### Checkpoint File
- `Varify_partner/checkpoint.json`: Saves progress for resuming interrupted processes

## Configuration

The script uses the following Odoo connection settings (configured in the script):

```python
CONFIG = {
    'server_url': 'http://mogth.work:8069',
    'database': 'MOG_SETUP',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    # ... other settings
}
```

## How It Works

1. **Connection**: Establishes connection to Odoo with retry logic
2. **Data Loading**: Reads partner codes from Excel file
3. **Batch Processing**: Processes codes in configurable batches
4. **Verification**: Searches each code first in the `old_code_partner` field, then falls back to `partner_code` field if not found
5. **Logging**: Logs found and not found partners with detailed search information
6. **Reporting**: Generates output files with results

## Error Handling

- **Network Errors**: Automatic retry with exponential backoff
- **API Errors**: Detailed logging and graceful degradation
- **Connection Timeouts**: Configurable timeout handling
- **Data Errors**: Skips invalid or empty partner codes

## Performance Considerations

- Default batch size is 100 records
- Configurable retry limits and delays
- Progress checkpoints every 50 records
- Connection health monitoring

## Example Output

```
2025-11-22 09:48:11,277 - INFO - Log file created: Varify_partner/logs/partner_verify_20251122_094811.log
2025-11-22 09:48:11,277 - INFO - DRY RUN MODE: Skipping Odoo connection
2025-11-22 09:48:11,419 - INFO - Starting verification from index 0
2025-11-22 09:48:11,419 - INFO - Total records to process: 634
2025-11-22 09:48:11,419 - INFO - Processing batch 0-10 of 634
...
2025-11-22 09:48:11,423 - INFO - Partner verification completed successfully!
```

## Troubleshooting

### Connection Issues
- Check network connectivity
- Verify Odoo server URL and credentials
- Check if Odoo server is accessible

### Data Issues
- Ensure Excel file has the correct format
- Check for empty or invalid partner codes
- Verify file permissions

### Performance Issues
- Adjust batch size based on server capacity
- Monitor server load during processing
- Use dry-run mode for testing

## Notes

- The script searches first in the `old_code_partner` field, then falls back to `partner_code` field if not found
- Empty or invalid partner codes are skipped automatically
- Progress is saved periodically for recovery
- All operations are logged for auditing purposes
- Detailed logging indicates which field was used to find each partner