import pandas as pd
import xmlrpc.client

# Odoo connection parameters
HOST = 'http://160.187.249.148:8069'
DB = 'Test_Module'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

# Excel file path
EXCEL_FILE = 'Recreate DP SO/SO_Recreate_Transfer.xlsx'

def main():
    print(f"Connecting to Odoo at {HOST}...")
    try:
        common = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/common')
        uid = common.authenticate(DB, USERNAME, PASSWORD, {})
    except Exception as e:
        print(f"Failed to connect to Odoo API: {e}")
        return

    if not uid:
        print("Authentication failed. Please check DB, USERNAME, and PASSWORD.")
        return
        
    print(f"Authenticated successfully with UID: {uid}")
    models = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/2/object')
    
    print(f"Reading Excel file: {EXCEL_FILE}")
    try:
        df = pd.read_excel(EXCEL_FILE)
    except Exception as e:
        print(f"Failed to read excel file. Please ensure 'pandas' and 'openpyxl' are installed. Error: {e}")
        return
        
    # Standardize column name just in case there are trailing spaces
    df.columns = df.columns.str.strip()
        
    if 'SO' not in df.columns:
        print(f"Error: Column 'SO' not found in the Excel file. Found columns: {df.columns.tolist()}")
        return
        
    so_list = df['SO'].dropna().unique()
    print(f"Found {len(so_list)} Sales Orders to process.")
    
    for so_name in so_list:
        try:
            print(f"\nProcessing SO: {so_name}")
            # Search for the sale order by name
            so_ids = models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'search', [[('name', '=', str(so_name).strip())]])
            
            if not so_ids:
                print(f"  - SO {so_name} not found in Odoo.")
                continue
                
            so_id = so_ids[0]
            
            # Read the state of the SO
            so_data = models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'read', [so_id], {'fields': ['state', 'name']})
            state = so_data[0].get('state')
            
            if state in ['draft', 'sent']:
                print(f"  - SO {so_name} state is '{state}'. Calling action_confirm to generate delivery transfers...")
                models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'action_confirm', [[so_id]])
                print(f"  - Successfully confirmed SO {so_name} and recreated delivery transfer.")
            elif state == 'cancel':
                print(f"  - SO {so_name} state is 'cancel'. Setting to draft and confirming...")
                models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'action_draft', [[so_id]])
                models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'action_confirm', [[so_id]])
                print(f"  - Successfully recreated delivery transfer for SO {so_name}.")
            elif state == 'sale':
                # If it's already confirmed, one standard approach is to trigger action_cancel, then action_draft, then action_confirm
                print(f"  - SO {so_name} is already confirmed ('sale' state). Canceling, drafting and then confirming...")
                try:
                    models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'action_cancel', [[so_id]])
                    models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'action_draft', [[so_id]])
                    models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'action_confirm', [[so_id]])
                    print(f"  - Execution for confirmed SO {so_name} completed. Delivery transfers recreated.")
                except Exception as inner_e:
                    # Alternative option: forcefully write 'draft' state if action_cancel is blocked
                    print(f"  - Error in cancel->draft->confirm: {inner_e}. Attempting direct state rewrite...")
                    models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'write', [[so_id], {'state': 'draft'}])
                    models.execute_kw(DB, uid, PASSWORD, 'sale.order', 'action_confirm', [[so_id]])
                    print(f"  - Successfully forced draft and confirmed SO {so_name}.")
            else:
                print(f"  - SO {so_name} state is '{state}'. Skipped.")
                
        except Exception as e:
            print(f"  - Error processing SO {so_name}: {e}")

if __name__ == "__main__":
    main()