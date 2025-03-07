import xmlrpc.client
import pandas as pd
import sys
import logging
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'url': 'http://mogth.work:8069',
    'db': 'Pre_Test',
    'username': 'apichart@mogen.co.th',
    'password': '471109538',
    'excel_path': 'Data_file/customer_import.xlsx'
}