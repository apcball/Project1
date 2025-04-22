import xmlrpc.client
import os
from PIL import Image
import base64

# --- Connection Settings ---
url = 'http://mogth.work:8069/'
db = 'MOG_LIVE'
username = 'apichart@mogen.co.th'
password = '471109538'

# XML-RPC endpoints
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

# Authenticate and get user id
uid = common.authenticate(db, username, password, {})

def get_image_binary(image_path):
    """Convert image to base64 binary."""
    try:
        with Image.open(image_path) as img:
            # Convert RGBA to RGB if necessary
            if img.mode == 'RGBA':
                img = img.convert('RGB')
            # Resize image if too large (optional)
            max_size = (1024, 1024)  # Maximum dimensions
            img.thumbnail(max_size, Image.Resampling.LANCZOS)
            
            # Save image to binary
            import io
            binary_data = io.BytesIO()
            img.save(binary_data, format=img.format or 'JPEG')
            binary_data = binary_data.getvalue()
            return base64.b64encode(binary_data).decode()
    except Exception as e:
        print(f"Error processing image {image_path}: {str(e)}")
        return None

def import_product_images():
    """Import product images from directory matching default_code."""
    image_dir = r'C:\Users\Ball\Documents\Git_apcball\Project1\image'
    
    # Get all products from Odoo
    products = models.execute_kw(db, uid, password,
        'product.template', 'search_read',
        [[]], {'fields': ['id', 'default_code']}
    )
    
    # Create dictionary of default_code to product_id
    product_dict = {p['default_code']: p['id'] for p in products if p['default_code']}
    
    # Process each image in directory
    updated_count = 0
    for filename in os.listdir(image_dir):
        # Get file name without extension
        name_without_ext = os.path.splitext(filename)[0]
        
        # Check if filename (without extension) matches any default_code
        if name_without_ext in product_dict:
            image_path = os.path.join(image_dir, filename)
            image_binary = get_image_binary(image_path)
            
            if image_binary:
                try:
                    # Update product image
                    models.execute_kw(db, uid, password,
                        'product.template', 'write',
                        [[product_dict[name_without_ext]], {
                            'image_1920': image_binary
                        }]
                    )
                    print(f"Successfully updated image for product with default_code: {name_without_ext}")
                    updated_count += 1
                except Exception as e:
                    print(f"Error updating product {name_without_ext}: {str(e)}")
    
    print(f"\nTotal products updated: {updated_count}")

if __name__ == "__main__":
    try:
        print("Starting product image import...")
        import_product_images()
        print("Import completed!")
    except Exception as e:
        print(f"An error occurred: {str(e)}")