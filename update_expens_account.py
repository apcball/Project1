#!/usr/bin/env python3
import os
import pandas as pd
import xmlrpc.client
import requests
import time, random, socket
from http.client import RemoteDisconnected
from xmlrpc.client import Fault, ProtocolError, ServerProxy, Transport

# ==== Odoo Connection ====
URL = 'http://119.59.124.100:8069'
DB = 'MOG_LIVE_15_08'
USERNAME = 'apichart@mogen.co.th'
PASSWORD = '471109538'

# ==== I/O ====
INPUT_PATH = 'Data_file\สินค้าระหว่างทาง.xlsx'   # คอลัมน์: Number, expense_account
