import xmlrpc.client
import os
from PIL import Image
import base64

# --- Connection Settings ---
url = 'http://mogth.work:8069/'
db = 'MOG_SETUP'
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
    """Import product images from directory matching SKU field."""
    image_dir = r'C:\Users\Ball\Documents\Git_apcball\Project1\image'
    
    # Get all product variants from Odoo
    products = models.execute_kw(db, uid, password,
        'product.product', 'search_read',
        [[]], {'fields': ['id', 'sku', 'product_tmpl_id']}
    )
    
    # Create dictionary of sku to product_tmpl_id
    product_dict = {p['sku']: p['product_tmpl_id'][0] for p in products if p.get('sku') and p['product_tmpl_id']}
    
    print(f"Total product variants in Odoo: {len(products)}")
    print(f"Variants with sku: {len(product_dict)}")
    
    # Get all image files
    image_files = os.listdir(image_dir)
    print(f"Total image files in directory: {len(image_files)}")
    
    # Process each image in directory
    updated_count = 0
    matched_count = 0
    for filename in image_files:
        # Get file name without extension
        name_without_ext = os.path.splitext(filename)[0]
        
        # Check if filename (without extension) matches any sku
        if name_without_ext in product_dict:
            matched_count += 1
            image_path = os.path.join(image_dir, filename)
            image_binary = get_image_binary(image_path)
            
            if image_binary:
                try:
                    # Update product template image
                    models.execute_kw(db, uid, password,
                        'product.template', 'write',
                        [[product_dict[name_without_ext]], {
                            'image_1920': image_binary
                        }]
                    )
                    print(f"Successfully updated image for product with SKU: {name_without_ext}")
                    updated_count += 1
                except Exception as e:
                    print(f"Error updating product {name_without_ext}: {str(e)}")
            else:
                print(f"Failed to process image for {name_without_ext}")
    
    print(f"\nMatched files: {matched_count}")
    print(f"Total products updated: {updated_count}")

if __name__ == "__main__":
    try:
        print("Starting product image import...")
        import_product_images()
        print("Import completed!")
    except Exception as e:
        print(f"An error occurred: {str(e)}")