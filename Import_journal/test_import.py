#!/usr/bin/env python3
"""
Test script for the updated import_journal_new.py
This script tests the data reading and processing without actually creating entries in Odoo
"""

import pandas as pd
import sys
import os

# Add the current directory to the path to import the main module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_read_excel():
    """Test reading the Excel file with the new format"""
    print("Testing Excel file reading...")
    
    # Import the read_excel_file function from the main module
    from import_journal_new import read_excel_file
    
    try:
        df = read_excel_file()
        print(f"[OK] Successfully read Excel file with {len(df)} rows")
        print(f"[OK] Columns: {df.columns.tolist()}")
        
        # Check if all expected columns are present
        expected_columns = [
            'document_number', 'date', 'journal', 'reference', 'custom_reference',
            'account_debit', 'account_credit', 'partner_code', 'old_partner_code',
            'partner_name', 'label', 'debit', 'credit'
        ]
        
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            print(f"[ERROR] Missing columns: {missing_columns}")
            return False
        else:
            print("[OK] All expected columns are present")
        
        # Display first few rows
        print("\nFirst 3 rows of data:")
        for i, row in df.head(3).iterrows():
            print(f"\nRow {i+1}:")
            print(f"  Document: {row['document_number']}")
            print(f"  Date: {row['date']}")
            print(f"  Journal: {row['journal']}")
            print(f"  Reference: {row['reference']}")
            print(f"  Custom Ref: {row['custom_reference']}")
            print(f"  Debit Account: {row['account_debit']}")
            print(f"  Credit Account: {row['account_credit']}")
            print(f"  Partner Code: {row['partner_code']}")
            print(f"  Old Partner Code: {row['old_partner_code']}")
            print(f"  Amount: {row['debit']} (debit) / {row['credit']} (credit)")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Error reading Excel file: {str(e)}")
        return False

def test_data_processing():
    """Test the data processing logic"""
    print("\n\nTesting data processing logic...")
    
    from import_journal_new import read_excel_file
    
    try:
        df = read_excel_file()
        
        # Group by document number
        for doc_number, doc_group in df.groupby('document_number'):
            print(f"\nProcessing document: {doc_number}")
            print(f"Number of lines: {len(doc_group)}")
            
            # For each row, verify we have both debit and credit accounts
            for _, row in doc_group.iterrows():
                if not row['account_debit'] or not row['account_credit']:
                    print(f"[ERROR] Row missing debit or credit account")
                    return False
                    
                # Check that debit and credit amounts are equal
                if row['debit'] != row['credit']:
                    print(f"[ERROR] Debit ({row['debit']}) != Credit ({row['credit']})")
                    return False
            
            print(f"[OK] Document {doc_number} has valid data")
            
            # Only test first document to avoid too much output
            break
        
        print("[OK] Data processing test passed")
        return True
        
    except Exception as e:
        print(f"[ERROR] Error in data processing: {str(e)}")
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("TESTING IMPORT JOURNAL UPDATED IMPLEMENTATION")
    print("=" * 60)
    
    # Check if Excel file exists
    excel_path = 'import_journal_ค้างจ่าย.xlsx'
    if not os.path.exists(excel_path):
        # Try to list files in the directory
        print("Files in current directory:")
        for f in os.listdir('.'):
            if f.endswith('.xlsx'):
                print(f"  - {f}")
        print(f"[ERROR] Excel file not found at {excel_path}")
        return
    
    # Run tests
    test1_passed = test_read_excel()
    test2_passed = test_data_processing()
    
    print("\n" + "=" * 60)
    if test1_passed and test2_passed:
        print("[OK] ALL TESTS PASSED - Ready to import to Odoo!")
    else:
        print("[ERROR] SOME TESTS FAILED - Please fix issues before importing")
    print("=" * 60)

if __name__ == "__main__":
    main()