import pandas as pd
from sqlalchemy.engine import Engine

def load_data_to_dw(df: pd.DataFrame, table_name: str, dw_engine: Engine):
    if df is None or df.empty:
        print(f"⚠️ Skip Load {table_name}: DataFrame kosong.")
        return

    print(f"--- 🚀 Mulai Load {table_name} ({len(df)} baris) ---")
    
    try:

        df.to_sql(
            table_name,
            con=dw_engine,
            if_exists='append',
            index=False,
            schema='northwind-dw'
        )
        print(f"✅ Load {table_name} berhasil.")
    except Exception as e:
        print(f"❌ GAGAL Load {table_name}. Cek DataFrames dan skema DB Anda. Error: {e}")

def load_all_data(transformed_data: dict[str, pd.DataFrame], dw_engine: Engine):
    print("\n" + "=" * 50)
    print("PHASE 3: LOADING DATA TO WAREHOUSE")
    print("=" * 50)
    
    dim_order = [
        'dim_date', 'dim_shipper', 'dim_customer', 
        'dim_employee', 'dim_product'
    ]

    for dim_name in dim_order:
        load_data_to_dw(transformed_data.get(dim_name), dim_name, dw_engine)

    fact_name = 'fact_sales'
    load_data_to_dw(transformed_data.get(fact_name), fact_name, dw_engine)
    
    print("\n" + "=" * 50)
    print("ETL PROCESS COMPLETED SUCCESSFULLY!")
    print("=" * 50)


load_data = load_all_data