# Code Specification for Import Journal Update

## 1. read_excel_file() Function Changes

### Current Implementation (lines 42-55):
```python
df.columns = [
    'document_number',    # number
    'date',              # accounting Date
    'journal',           # journal
    'reference',         # reference
    'account1',          # account1
    'account2',          # account2
    'partner_code',      # partner_code
    'partner_name',      # partner_id
    'label',            # label
    'debit',            # debit
    'credit'            # credit
]
```

### New Implementation:
```python
df.columns = [
    'document_number',      # number
    'date',                # accounting Date
    'journal',             # journal
    'reference',           # reference
    'custom_reference',    # custom_referrence
    'account_debit',       # account_debit
    'account_credit',      # account_credit
    'partner_code',        # partner_code
    'old_partner_code',    # old_partner_code
    'partner_name',        # partner_name
    'label',               # label
    'debit',               # debit
    'credit'               # credit
]
```

### Additional Cleaning Required:
```python
# Clean new columns
df['custom_reference'] = df['custom_reference'].fillna('').astype(str).str.strip()
df['old_partner_code'] = df['old_partner_code'].fillna('').astype(str).str.strip()
```

## 2. find_partner_by_code() Function Changes

### Current Implementation (lines 166-199):
- Only searches by partner_code and partner_name

### New Implementation:
Add old_partner_code as fallback:
```python
def find_partner_by_code(uid, models, partner_code, old_partner_code=None, partner_name=None):
    # Try partner_code first
    partner_id = models.execute_kw(db, uid, password,
        'res.partner', 'search',
        [[['partner_code', '=', partner_code]]])
    
    if not partner_id and old_partner_code:
        # Try old_partner_code
        partner_id = models.execute_kw(db, uid, password,
            'res.partner', 'search',
            [[['partner_code', '=', old_partner_code]]])
    
    # Rest of the function remains the same...
```

## 3. process_document_group() Function Changes

### Current Implementation (lines 218-242):
Creates one journal line per row.

### New Implementation:
Create two journal lines per row:

```python
# Process each line in the document group
for _, row in doc_group.iterrows():
    # Skip rows without accounts
    if (pd.isna(row['account_debit']) or str(row['account_debit']).strip() == '' or 
        pd.isna(row['account_credit']) or str(row['account_credit']).strip() == ''):
        continue

    # Find debit account
    debit_account = find_account_by_code(uid, models, row['account_debit'])
    if not debit_account:
        print(f"Could not find debit account: {row['account_debit']}")
        continue
        
    # Find credit account
    credit_account = find_account_by_code(uid, models, row['account_credit'])
    if not credit_account:
        print(f"Could not find credit account: {row['account_credit']}")
        continue

    # Find partner
    partner = find_partner_by_code(uid, models, row['partner_code'], 
                                  row['old_partner_code'], row['partner_name'])

    # Create debit line
    amount = float(row['debit']) if not pd.isna(row['debit']) else float(row['credit'])
    debit_line = {
        'account_id': debit_account['id'],
        'name': row['label'] or str(row['document_number']).strip(),
        'debit': amount,
        'credit': 0.0,
    }
    if partner:
        debit_line['partner_id'] = partner['id']
    move_lines.append((0, 0, debit_line))
    
    # Create credit line
    credit_line = {
        'account_id': credit_account['id'],
        'name': row['label'] or str(row['document_number']).strip(),
        'debit': 0.0,
        'credit': amount,
    }
    if partner:
        credit_line['partner_id'] = partner['id']
    move_lines.append((0, 0, credit_line))
```

## 4. Move Data Creation Changes

### Current Implementation (lines 254-260):
```python
move_data = {
    'ref': str(first_row['document_number']).strip(),
    'name': str(first_row['document_number']).strip(),
    'date': first_row['date'].strftime('%Y-%m-%d'),
    'journal_id': journal['id'],
    'line_ids': move_lines,
}
```

### New Implementation:
Add custom_reference:
```python
move_data = {
    'ref': str(first_row['reference']).strip() if first_row['reference'] else str(first_row['document_number']).strip(),
    'name': str(first_row['document_number']).strip(),
    'date': first_row['date'].strftime('%Y-%m-%d'),
    'journal_id': journal['id'],
    'line_ids': move_lines,
}
# Add custom_reference if it exists
if first_row['custom_reference']:
    move_data['custom_reference'] = str(first_row['custom_reference']).strip()
```

## Testing Requirements

1. Verify all 13 columns are read correctly
2. Verify two lines are created per Excel row
3. Verify debit and credit accounts are correctly assigned
4. Verify partner lookup with old_partner_code fallback
5. Verify custom_reference is saved in move data
6. Verify journal entries are balanced (debit = credit)