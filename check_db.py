
import xmlrpc.client

HOST = 'http://160.187.249.148:8069'

try:
    print(f"Connecting to {HOST}...")
    db = xmlrpc.client.ServerProxy(f'{HOST}/xmlrpc/db')
    dbs = db.list()
    print("Available databases:", dbs)
except Exception as e:
    print(f"Error listing databases: {e}")
