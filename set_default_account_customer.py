import xmlrpc.client

# --- Connection Settings ---
url = 'http://mogth.work:8069'
db = 'MOG_SETUP'
username = 'apichart@mogen.co.th'
password = '471109538'

# XML-RPC endpoints
common_endpoint = f'{url}/xmlrpc/2/common'
object_endpoint = f'{url}/xmlrpc/2/object'

# Create XML-RPC clients
common = xmlrpc.client.ServerProxy(common_endpoint)
models = xmlrpc.client.ServerProxy(object_endpoint)

try:
    # Authenticate and get user id
    uid = common.authenticate(db, username, password, {})
    if not uid:
        raise Exception("Authentication failed")
    
    print(f"Successfully authenticated with user ID: {uid}")

    # Define the account details
    accounts_to_create = [
        {
            'code': '114200',
            'name': 'ลูกหนี้การค้า',
            'user_type_id': models.execute_kw(db, uid, password,
                'account.account.type', 'search',
                [[('name', '=', 'Receivable')]], {'limit': 1}
            )[0],
            'reconcile': True,
            'internal_type': 'receivable',
        },
        {
            'code': '211200',
            'name': 'เจ้าหนี้การค้า',
            'user_type_id': models.execute_kw(db, uid, password,
                'account.account.type', 'search',
                [[('name', '=', 'Payable')]], {'limit': 1}
            )[0],
            'reconcile': True,
            'internal_type': 'payable',
        }
    ]

    account_ids = {}
    
    # Get company ID
    company_id = models.execute_kw(db, uid, password,
        'res.company', 'search',
        [[]], {'limit': 1}
    )[0]

    # Create or get accounts
    for account in accounts_to_create:
        # Check if account exists
        existing_account = models.execute_kw(db, uid, password,
            'account.account', 'search',
            [[('code', '=', account['code']), ('company_id', '=', company_id)]],
            {'limit': 1}
        )

        if existing_account:
            account_ids[account['code']] = existing_account[0]
            print(f"Account {account['code']} already exists")
        else:
            # Add company_id to account data
            account['company_id'] = company_id
            
            # Create new account
            new_account_id = models.execute_kw(db, uid, password,
                'account.account', 'create',
                [account]
            )
            account_ids[account['code']] = new_account_id
            print(f"Created new account {account['code']}")

    # Update or create property fields
    for field, account_code in [
        ('property_account_receivable_id', '114200'),
        ('property_account_payable_id', '211200')
    ]:
        # Search for existing property
        existing_property = models.execute_kw(db, uid, password,
            'ir.property', 'search',
            [[
                ('name', '=', field),
                ('company_id', '=', company_id)
            ]]
        )

        if existing_property:
            # Update existing property
            models.execute_kw(db, uid, password, 'ir.property', 'write', [
                existing_property,
                {'value_reference': f'account.account,{account_ids[account_code]}'}
            ])
            print(f"Updated {field} to account {account_code}")
        else:
            # Create new property
            models.execute_kw(db, uid, password, 'ir.property', 'create', [{
                'name': field,
                'fields_id': models.execute_kw(db, uid, password,
                    'ir.model.fields', 'search',
                    [[('name', '=', field), ('model', '=', 'res.partner')]],
                    {'limit': 1}
                )[0],
                'value_reference': f'account.account,{account_ids[account_code]}',
                'type': 'many2one',
                'res_id': False,  # This makes it a default value
                'company_id': company_id
            }])
            print(f"Created new property {field} with account {account_code}")

    print("Successfully set up accounts and default properties")

except Exception as e:
    print(f"An error occurred: {str(e)}")