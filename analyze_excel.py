import pandas as pd
import logging
import os
from datetime import datetime

# Excel file path
EXCEL_FILE = 'Data_file/สิ้นเปลืองโรงงาน.xlsx'

# Configure logging
log_filename = f'excel_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def analyze_excel():
    """Analyze the Excel file structure and content"""
    try:
        if not os.path.exists(EXCEL_FILE):
            logger.error(f"Excel file not found: {EXCEL_FILE}")
            return
        
        # Read the Excel file
        logger.info(f"Reading Excel file: {EXCEL_FILE}")
        df = pd.read_excel(EXCEL_FILE)
        
        # Basic file info
        logger.info(f"Excel file has {len(df)} rows and {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Check for sequence column
        if 'sequence' in df.columns:
            logger.info("Found 'sequence' column")
            logger.info(f"sequence dtype: {df['sequence'].dtype}")
            logger.info(f"sequence unique values: {sorted(df['sequence'].unique().tolist())}")
            logger.info(f"sequence min: {df['sequence'].min()}, max: {df['sequence'].max()}")
            
            # Check for duplicates
            duplicates = df['sequence'].duplicated().sum()
            logger.info(f"Found {duplicates} duplicate sequence values")
            
            # Check for NaN values
            nan_count = df['sequence'].isna().sum()
            logger.info(f"Found {nan_count} NaN sequence values")
            
        # Analyze price_unit column specifically
        if 'price_unit' in df.columns:
            logger.info("Found 'price_unit' column")
            logger.info(f"price_unit dtype: {df['price_unit'].dtype}")
            
            # Count zeros and NaN values
            zeros = (df['price_unit'] == 0).sum()
            logger.info(f"Found {zeros} rows with price_unit = 0")
            
            nans = df['price_unit'].isna().sum()
            logger.info(f"Found {nans} rows with NaN price_unit")
            
            negatives = (df['price_unit'] < 0).sum()
            logger.info(f"Found {negatives} rows with negative price_unit")
            
            # Check data types within the column
            data_types = {type(x) for x in df['price_unit'].dropna()}
            logger.info(f"price_unit column contains these data types: {data_types}")
            
            # Show sample values
            logger.info(f"Sample price_unit values: {df['price_unit'].head().tolist()}")
            
        # Analyze product_id column
        if 'product_id' in df.columns:
            logger.info("Found 'product_id' column")
            logger.info(f"product_id dtype: {df['product_id'].dtype}")
            sample_products = df['product_id'].sample(min(5, len(df))).tolist()
            logger.info(f"Sample product_id values: {sample_products}")
            
        # Analyze quantity column
        if 'product_uom_qty' in df.columns:
            logger.info("Found 'product_uom_qty' column")
            logger.info(f"product_uom_qty dtype: {df['product_uom_qty'].dtype}")
            logger.info(f"product_uom_qty min: {df['product_uom_qty'].min()}, max: {df['product_uom_qty'].max()}")
            logger.info(f"Sample product_uom_qty values: {df['product_uom_qty'].head().tolist()}")
            
        # Analyze location column
        if 'location_dest_id' in df.columns:
            logger.info("Found 'location_dest_id' column")
            logger.info(f"location_dest_id dtype: {df['location_dest_id'].dtype}")
            unique_locations = df['location_dest_id'].unique().tolist()
            logger.info(f"Unique location_dest_id values: {unique_locations}")
            
        # Calculate total valuation based on price_unit * product_uom_qty
        if 'price_unit' in df.columns and 'product_uom_qty' in df.columns:
            df['calculated_value'] = df['price_unit'] * df['product_uom_qty']
            total_value = df['calculated_value'].sum()
            logger.info(f"Total calculated valuation: {total_value}")
            
            # Group by sequence and show price_unit stats
            price_stats = df.groupby('sequence')['price_unit'].agg(['mean', 'min', 'max', 'count'])
            logger.info("Price unit statistics by sequence:")
            logger.info(price_stats.head(10).to_string())
            
            # Show rows with zero price_unit values
            zero_price_indices = df[df['price_unit'] == 0].index.tolist()
            logger.info(f"Index of rows with zero price_unit: {zero_price_indices[:20]} {'...' if len(zero_price_indices) > 20 else ''}")
        else:
            logger.info("No 'sequence' column found")
        
        # Check for price_unit column
        if 'price_unit' in df.columns:
            logger.info("Found 'price_unit' column")
            logger.info(f"price_unit dtype: {df['price_unit'].dtype}")
            
            # Check for zero values
            zero_count = (df['price_unit'] == 0).sum()
            logger.info(f"Found {zero_count} rows with price_unit = 0")
            
            # Check for NaN values
            nan_count = df['price_unit'].isna().sum()
            logger.info(f"Found {nan_count} rows with NaN price_unit")
            
            # Check for negative values
            neg_count = (df['price_unit'] < 0).sum()
            logger.info(f"Found {neg_count} rows with negative price_unit")
            
            # Check for positive values
            pos_count = (df['price_unit'] > 0).sum()
            logger.info(f"Found {pos_count} rows with positive price_unit")
            
            # Sample of price_unit values
            logger.info(f"Sample price_unit values: {df['price_unit'].head(10).tolist()}")
            
            # Check data types in the column
            price_types = set(type(x) for x in df['price_unit'])
            logger.info(f"price_unit column contains these data types: {price_types}")
            
            # If there are string values, show examples
            string_values = [x for x in df['price_unit'] if isinstance(x, str)]
            if string_values:
                logger.info(f"Found {len(string_values)} string values in price_unit column. Examples: {string_values[:5]}")
        else:
            logger.info("No 'price_unit' column found")
        
        # Check for product_id column
        if 'product_id' in df.columns:
            logger.info("Found 'product_id' column")
            logger.info(f"product_id dtype: {df['product_id'].dtype}")
            logger.info(f"Number of unique products: {df['product_id'].nunique()}")
            logger.info(f"Sample product_id values: {df['product_id'].head(5).tolist()}")
        else:
            logger.info("No 'product_id' column found")
        
        # Check for product_uom_qty column
        if 'product_uom_qty' in df.columns:
            logger.info("Found 'product_uom_qty' column")
            logger.info(f"product_uom_qty dtype: {df['product_uom_qty'].dtype}")
            logger.info(f"product_uom_qty min: {df['product_uom_qty'].min()}, max: {df['product_uom_qty'].max()}")
            logger.info(f"Sample product_uom_qty values: {df['product_uom_qty'].head(5).tolist()}")
        else:
            logger.info("No 'product_uom_qty' column found")
        
        # Check for location_dest_id column
        if 'location_dest_id' in df.columns:
            logger.info("Found 'location_dest_id' column")
            logger.info(f"location_dest_id dtype: {df['location_dest_id'].dtype}")
            logger.info(f"Unique location_dest_id values: {df['location_dest_id'].unique().tolist()}")
        else:
            logger.info("No 'location_dest_id' column found")
        
        # Check for scheduled_date column
        if 'scheduled_date' in df.columns:
            logger.info("Found 'scheduled_date' column")
            logger.info(f"scheduled_date dtype: {df['scheduled_date'].dtype}")
            logger.info(f"Sample scheduled_date values: {df['scheduled_date'].head(5).tolist()}")
        else:
            logger.info("No 'scheduled_date' column found")
        
        # Check for any correlation between sequence and price_unit
        if 'sequence' in df.columns and 'price_unit' in df.columns:
            # Group by sequence and check price_unit values
            sequence_groups = df.groupby('sequence')['price_unit'].agg(['mean', 'min', 'max', 'count'])
            logger.info(f"Price unit statistics by sequence:\n{sequence_groups.head(10)}")
            
            # Check if there's a pattern where price_unit is 0 for certain sequences
            zero_price_sequences = df[df['price_unit'] == 0]['sequence'].unique()
            if len(zero_price_sequences) > 0:
                logger.info(f"Sequences with zero price_unit: {sorted(zero_price_sequences)}")
        
    except Exception as e:
        logger.error(f"Error analyzing Excel file: {str(e)}")

if __name__ == "__main__":
    analyze_excel()