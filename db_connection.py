import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()

def conn() -> Engine:

    db_user = os.getenv("PG_USER")
    db_password = os.getenv("PG_PASSWORD")
    db_host = os.getenv("PG_HOST")
    db_port = os.getenv("PG_PORT")
    db_name = os.getenv("PG_DATABASE")

    db_schema = os.getenv("PG_SCHEMA") 

    database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:

        engine = create_engine(
            database_url,
            connect_args={'options': f'-csearch_path={db_schema}'}
        )
        conn = engine.connect()
        conn.close()
        print(f"Koneksi ke Data Warehouse '{db_name}' pada skema '{db_schema}' berhasil! ✅")
        return engine
    except Exception as e:
        print(f"Gagal terhubung ke database: {e}")
        return None

if __name__ == "__main__":
    dw_engine = conn()
    if dw_engine:
        print(f"Engine berhasil dibuat: {dw_engine}")
    else:
        print("Engine gagal dibuat.")