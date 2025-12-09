import pandas as pd
import os

def extract_data(data_folder='data'):
    print("=" * 50)
    print("PHASE 1: EXTRACTING DATA")
    print("=" * 50)
    
    data = {}
    
    files = {
        'orders': 'orders.csv',
        'order_details': 'order_details.csv',
        'products': 'products.csv',
        'categories': 'categories.csv',
        'customers': 'customers.csv',
        'employees': 'employees.csv',
        'shippers': 'shippers.csv',
        'suppliers': 'suppliers.csv'
    }
    
    for key, filename in files.items():
        filepath = os.path.join(data_folder, filename)
        try:
            df = pd.read_csv(filepath)
            data[key] = df
            print(f"✓ Loaded {key}: {len(df)} rows")
        except FileNotFoundError:
            print(f"✗ File not found: {filepath}")
            raise
    
    print(f"\nTotal tables loaded: {len(data)}")
    return data