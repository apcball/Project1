import xmlrpc.client

HOST = 'http://mogth.work:8069'
DB = 'MOG_Training'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(HOST))
uid = common.authenticate(DB, USERNAME, PASSWORD, {})

models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(HOST))

# ค้นหา session ของผู้ใช้
user_id = models.execute_kw(DB, uid, PASSWORD, 'res.users', 'search', [[['login', '=', 'ชื่อผู้ใช้']]])
if user_id:
    models.execute_kw(DB, uid, PASSWORD, 'ir.sessions', 'unlink', [user_id])
    print("User session terminated.")
else:
    print("User not found.")