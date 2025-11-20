# Implementation Plan for Import Journal Update

## Overview
Update `import_journal_new.py` to handle the new Excel format with 13 columns and create two journal lines per row (debit and credit).

## Key Changes Required

### 1. Column Mapping Update
Current implementation expects 11 columns:
- document_number, date, journal, reference, account1, account2, partner_code, partner_name, label, debit, credit

New format has 13 columns:
- document_number, date, journal, reference, custom_reference, account_debit, account_credit, partner_code, old_partner_code, partner_name, label, debit, credit

### 2. Journal Line Creation Logic
Current: Creates one journal line per row
New: Creates TWO journal lines per row:
- Line 1: Debit entry using account_debit
- Line 2: Credit entry using account_credit

### 3. New Fields Support
- custom_reference: Store in move data
- old_partner_code: Use as fallback when partner_code not found

## Implementation Steps

### Step 1: Update read_excel_file() function
- Change column mapping to handle 13 columns
- Add cleaning for new columns (custom_reference, old_partner_code)
- Ensure proper data type conversion

### Step 2: Update process_document_group() function
- Modify to create two lines per row
- Use account_debit for debit line
- Use account_credit for credit line
- Both lines use the same amount (debit or credit value from the row)

### Step 3: Update find_partner_by_code() function
- Add old_partner_code as fallback search criteria
- Try partner_code first, then old_partner_code if not found

### Step 4: Update move data creation
- Include custom_reference field
- Ensure proper balancing of debit and credit

## Data Flow Example

For each Excel row:
```
document_number: BCADV6810011
date: 2025-10-28
journal: สมุดรายวันค้างจ่าย
reference: PTPT00047370
custom_reference: OB-ค้างจ่าย
account_debit: 900001
account_credit: 214102
partner_code: (empty)
old_partner_code: V00010
partner_name: (empty)
label: -ค่าแรง+จานครัช ซ่อมบำรุงรถ ถน 9782 กทม
debit: 1335
credit: 1335
```

Will create TWO journal lines:
1. Debit line: Account 900001, Debit 1335
2. Credit line: Account 214102, Credit 1335

## Testing Plan
1. Test with sample Excel file
2. Verify proper account lookup
3. Verify partner lookup with fallback
4. Verify balanced journal entries
5. Verify custom_reference is saved