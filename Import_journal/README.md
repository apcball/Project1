# Import Journal Entries - Updated Implementation

## Overview
This script imports journal entries from an Excel file into Odoo using the XML-RPC API. The updated version supports a new Excel format with 13 columns and creates balanced journal entries with debit and credit lines.

## Key Features
- Imports journal entries from Excel to Odoo
- Creates balanced debit/credit entries (2 lines per Excel row)
- Supports partner lookup with fallback mechanism
- Handles custom references
- Validates account and journal existence

## Excel File Format
The Excel file must contain the following 13 columns in order:

| Column | Field Name | Description |
|--------|------------|-------------|
| 1 | document_number | Document number |
| 2 | date | Accounting date |
| 3 | journal | Journal code or name |
| 4 | reference | Reference field |
| 5 | custom_reference | Custom reference |
| 6 | account_debit | Debit account code |
| 7 | account_credit | Credit account code |
| 8 | partner_code | Partner code |
| 9 | old_partner_code | Old partner code (fallback) |
| 10 | partner_name | Partner name |
| 11 | label | Line description |
| 12 | debit | Debit amount |
| 13 | credit | Credit amount |

## How It Works
1. Reads Excel file with the specified format
2. For each row, creates TWO journal lines:
   - Debit line using account_debit
   - Credit line using account_credit
3. Looks up partners using partner_code, with old_partner_code as fallback
4. Creates balanced journal entries in Odoo

## Configuration
Update the connection settings at the top of the script:
- URL: Odoo server URL
- Database: Odoo database name
- Username: Odoo username
- Password: Odoo password

## Usage
```bash
python import_journal_new.py
```

## Files Created
- `implementation_plan.md`: Detailed implementation steps
- `data_flow_diagram.md`: Visual representation of data flow
- `code_specification.md`: Detailed code changes required

## Important Notes
- Each Excel row generates exactly two journal lines (debit and credit)
- The debit and credit amounts must be equal for balanced entries
- Accounts must exist in Odoo before import
- Partners are matched by code first, then by old_partner_code as fallback