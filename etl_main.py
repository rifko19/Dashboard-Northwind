# Pastikan semua file diimpor dengan nama yang benar
from db_connection import get_dw_engine
from exctract import extract_data
from transform import transform_all_data
from load import load_all_data

def run_etl():
    print("=" * 50)
    print("=== START: Northwind Data Warehouse ETL Pipeline ===")
    print("=" * 50)

    print("\n--- FASE: KONEKSI ---")
    dw_engine = get_dw_engine()
    if dw_engine is None:
        print("❌ ETL DIBATALKAN: Koneksi database gagal.")
        return

    print("\n--- FASE: EKSTRAKSI ---")
    try:
        raw_data = extract_data() 
    except Exception as e:
        print(f"❌ ETL DIBATALKAN: Ekstraksi data gagal. Error: {e}")
        return

    # 3. Transform
    print("\n--- FASE: TRANSFORMASI ---")
    transformed_data = transform_all_data(raw_data)
    if not transformed_data:
        print("❌ ETL DIBATALKAN: Transformasi menghasilkan data kosong.")
        return
    
    # 4. Load
    print("\n--- FASE: PEMUATAN (LOAD) ---")
    load_all_data(transformed_data, dw_engine)

    # Menutup koneksi
    if dw_engine:
        dw_engine.dispose()

    print("=" * 50)
    print("=== SUCCESS: ETL Pipeline Selesai! ✅            ===")
    print("=" * 50)

if __name__ == "__main__":
    run_etl()