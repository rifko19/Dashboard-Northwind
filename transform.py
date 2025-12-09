import pandas as pd
import numpy as np
from typing import Dict, Any


def get_normalized_data(raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    normalized_data = {}
    for key, df in raw_data.items():
        temp_df = df.copy()
        temp_df.columns = temp_df.columns.str.lower()
        normalized_data[key] = temp_df
    return normalized_data

def add_missing_columns(df: pd.DataFrame, required_cols: list) -> pd.DataFrame:
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.Series([np.nan] * len(df), index=df.index) 
    return df

def transform_all_data(raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    
    print("\n" + "=" * 50)
    print("PHASE 2: TRANSFORMING DATA")
    print("=" * 50)
    
    transformed_data = {}
    data = get_normalized_data(raw_data)

    # 1. DIMENSION: SHIPPER
    print("\n1. Transforming dim_shipper...")
    dim_shipper = data['shippers'].copy()
    dim_shipper['shipper_key'] = dim_shipper.index + 1
    
    dim_shipper = dim_shipper[[
        'shipper_key', 'shipperid', 'companyname', 'phone'
    ]].rename(columns={
        'shipperid': 'shipper_id',
        'companyname': 'company_name'
    })
    transformed_data['dim_shipper'] = dim_shipper
    print(f"   ✓ dim_shipper: {len(dim_shipper)} records")

    # 2. DIMENSION: CUSTOMER
    print("\n2. Transforming dim_customer...")
    dim_customer = data['customers'].copy()
    dim_customer['customer_key'] = dim_customer.index + 1

    required_customer_cols = ['region', 'postalcode', 'fax']
    dim_customer = add_missing_columns(dim_customer, required_customer_cols)
    
    dim_customer = dim_customer[[
        'customer_key', 'customerid', 'companyname', 'contactname', 'contacttitle',
        'address', 'city', 'region', 'postalcode', 'country', 'phone', 'fax'
    ]].rename(columns={
        'customerid': 'customer_id', 
        'companyname': 'company_name',
        'contactname': 'contact_name',
        'contacttitle': 'contact_title',
        'postalcode': 'postal_code'
    })
    dim_customer['region'] = dim_customer['region'].fillna('Unknown')
    dim_customer['postal_code'] = dim_customer['postal_code'].fillna('Unknown')
    transformed_data['dim_customer'] = dim_customer
    print(f"   ✓ dim_customer: {len(dim_customer)} records")
    
    # 3. DIMENSION: EMPLOYEE (FIX: UndefinedColumn 'last_name')
    print("\n3. Transforming dim_employee...")
    dim_employee = data['employees'].copy()
    dim_employee['employee_key'] = dim_employee.index + 1
    
    required_employee_cols = ['region', 'reportsto', 'salary', 'titleofcourtesy', 'homephone']
    dim_employee = add_missing_columns(dim_employee, required_employee_cols)

    dim_employee = dim_employee.rename(columns={
        'lastname': 'last_name',
        'firstname': 'first_name'
    }, errors='ignore')


    dim_employee['full_name'] = dim_employee['first_name'] + ' ' + dim_employee['last_name']

    dim_employee['birthdate'] = pd.to_datetime(dim_employee['birthdate'], errors='coerce')
    dim_employee['hiredate'] = pd.to_datetime(dim_employee['hiredate'], errors='coerce')
    dim_employee['salary'] = pd.to_numeric(dim_employee['salary'], errors='coerce').fillna(0.0)

    supervisor_map = dim_employee.set_index('employeeid')['full_name'].to_dict()
    dim_employee['reports_to_name'] = dim_employee['reportsto'].map(supervisor_map).fillna('N/A')

    dim_employee = dim_employee[[
        'employee_key', 'employeeid', 'last_name', 'first_name', 'full_name', 'title',
        'titleofcourtesy', 'birthdate', 'hiredate', 'address', 'city', 'region',
        'country', 'homephone', 'reportsto', 'reports_to_name', 'salary'
    ]].rename(columns={
        'employeeid': 'employee_id',
        'titleofcourtesy': 'title_of_courtesy',
        'birthdate': 'birth_date',
        'hiredate': 'hire_date',
        'homephone': 'home_phone',
        'reportsto': 'reports_to'
    })

    transformed_data['dim_employee'] = dim_employee
    print(f"   ✓ dim_employee: {len(dim_employee)} records")
    
    
    # 4. DIMENSION: PRODUCT
    print("\n4. Transforming dim_product...")
    
    dim_product = data['products'].merge(
        data['categories'], on='categoryid', how='left'
    ).merge(
        data['suppliers'], on='supplierid', how='left', suffixes=('_prod', '_sup')
    )
    
    dim_product['product_key'] = dim_product.index + 1

    desired_cols_prod = [
        'product_key', 'productid', 'productname', 'categoryid', 'categoryname', 
        'categorydescription', 'supplierid', 'companyname_sup', 'contactname_sup', 
        'country_sup', 'phone_sup', 'quantityperunit', 'unitprice', 'unitsinstock', 
        'unitsonorder', 'reorderlevel', 'discontinued'
    ]
    dim_product = add_missing_columns(dim_product, desired_cols_prod)

    dim_product = dim_product[desired_cols_prod].rename(columns={
        'productid': 'product_id',
        'productname': 'product_name',
        'categoryid': 'category_id',
        'categoryname': 'category_name',
        'categorydescription': 'category_description',
        'supplierid': 'supplier_id',
        'companyname_sup': 'supplier_name',
        'contactname_sup': 'supplier_contact_name',
        'country_sup': 'supplier_country',
        'phone_sup': 'supplier_phone',
        'quantityperunit': 'quantity_per_unit',
        'unitprice': 'unit_price',
        'unitsinstock': 'units_in_stock',
        'unitsonorder': 'units_on_order',
        'reorderlevel': 'reorder_level'
    })
    
    supplier_cols = ['supplier_name', 'supplier_contact_name', 'supplier_country', 'supplier_phone']
    dim_product[supplier_cols] = dim_product[supplier_cols].fillna('Unknown')
    
    dim_product['category_name'] = dim_product['category_name'].fillna('Unknown')
    dim_product['units_in_stock'] = dim_product['units_in_stock'].fillna(0).astype(int)
    dim_product['discontinued'] = dim_product['discontinued'].fillna(0).astype(bool)

    transformed_data['dim_product'] = dim_product
    print(f"   ✓ dim_product: {len(dim_product)} records")
    
    # 5. DIMENSION: DATE
    print("\n5. Transforming dim_date...")
    orders_df = data['orders']
    all_dates = pd.concat([
        pd.to_datetime(orders_df['orderdate'], errors='coerce'),
        pd.to_datetime(orders_df['shippeddate'], errors='coerce')
    ]).dropna().unique()
    
    dim_date = pd.DataFrame({'full_date': pd.to_datetime(all_dates)})
    dim_date = dim_date.drop_duplicates().sort_values('full_date').reset_index(drop=True)
    
    t = dim_date['full_date'].dt
    dim_date['date_key'] = t.strftime('%Y%m%d').astype(int) 
    dim_date['year'] = t.year
    dim_date['quarter'] = t.quarter
    dim_date['month'] = t.month
    dim_date['month_name'] = t.month_name()
    dim_date['day'] = t.day
    dim_date['day_of_week'] = t.dayofweek + 1
    dim_date['day_name'] = t.day_name()
    dim_date['week_of_year'] = t.isocalendar().week.astype(int)
    dim_date['is_weekend'] = t.dayofweek.isin([5, 6])
    dim_date['is_holiday'] = False
    
    transformed_data['dim_date'] = dim_date
    print(f"   ✓ dim_date: {len(dim_date)} records")

    # =======================================================
    # II. TRANSFORM FACT TABLE (Fokus Perbaikan)
    # =======================================================
    print("\n6. Transforming fact_sales and performing lookups...")
    
    fact_sales = data['orders'].merge(
        data['order_details'], on='orderid', how='inner'
    )
    
    fact_sales['total_sales'] = (
        fact_sales['quantity'] * fact_sales['unitprice'] * (1 - fact_sales['discount'])
    ).round(2)
    fact_sales['revenue'] = fact_sales['total_sales'] 
    
    
    # 1. Lookup Customer Key
    fact_sales = fact_sales.merge(
        transformed_data['dim_customer'][['customer_id', 'customer_key']],
        left_on='customerid', 
        right_on='customer_id', 
        how='left'
    ).drop(columns=['customer_id']) 
    
    # 2. Lookup Product Key
    fact_sales = fact_sales.merge(
        transformed_data['dim_product'][['product_id', 'product_key']],
        left_on='productid',
        right_on='product_id',
        how='left'
    ).drop(columns=['product_id'])

    # 3. Lookup Employee Key
    fact_sales = fact_sales.merge(
        transformed_data['dim_employee'][['employee_id', 'employee_key']],
        left_on='employeeid',
        right_on='employee_id',
        how='left'
    ).drop(columns=['employee_id'])

    # 4. Lookup Shipper Key
    fact_sales = fact_sales.merge(
        transformed_data['dim_shipper'][['shipper_id', 'shipper_key']],
        left_on='shipvia',
        right_on='shipper_id',
        how='left'
    ).drop(columns=['shipper_id'])
    
    # 5. Lookup Date Key (Order Date)
    dim_date_lookup = transformed_data['dim_date'].copy()
    dim_date_lookup['full_date_dt'] = pd.to_datetime(dim_date_lookup['full_date'])

    fact_sales['orderdate_dt'] = pd.to_datetime(fact_sales['orderdate'], errors='coerce')
    
    fact_sales = fact_sales.merge(
        dim_date_lookup[['full_date_dt', 'date_key']],
        left_on='orderdate_dt',
        right_on='full_date_dt',
        how='left'
    ).rename(columns={'date_key': 'date_key'})
    
    fact_sales = fact_sales.drop(columns=['orderdate_dt', 'full_date_dt'], errors='ignore')

    fact_sales = fact_sales.drop(columns=['customerid', 'productid', 'employeeid', 'shipvia', 'orderdate'], errors='ignore')

    fact_sales = fact_sales[[
        'orderid', 'customer_key', 'product_key', 'date_key', 
        'employee_key', 'shipper_key', 'unitprice', 'quantity', 'discount', 
        'total_sales', 'revenue', 'freight'
    ]].rename(columns={
        'orderid': 'order_id',
        'unitprice': 'unit_price',
    })

    fact_sales = fact_sales.dropna(subset=[
        'customer_key', 
        'product_key', 
        'date_key', 
        'employee_key', 
        'shipper_key'
    ])
    
    transformed_data['fact_sales'] = fact_sales
    print(f"   ✓ fact_sales: {len(fact_sales)} records (setelah hapus NULL FK)")
    
    


    for dim_name in ['dim_shipper', 'dim_customer', 'dim_employee', 'dim_product']:
        df = transformed_data[dim_name]
        
        # Kolom Business Key dihapus dari Dimensi
        if dim_name == 'dim_shipper':
            df = df.drop(columns=['shipperid'], errors='ignore')
        elif dim_name == 'dim_customer':
            df = df.drop(columns=['customerid'], errors='ignore')
        elif dim_name == 'dim_employee':
            df = df.drop(columns=['employeeid'], errors='ignore')
        elif dim_name == 'dim_product':
            df = df.drop(columns=['productid'], errors='ignore')
            
        transformed_data[dim_name] = df


    print(f"\n✓ Transformation completed successfully. Data ready for loading.")
    return transformed_data