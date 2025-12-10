🚀 Northwind Strategic Dashboard
Northwind Strategic Dashboard adalah sistem Business Intelligence berbasis Streamlit untuk menganalisis kinerja strategis perusahaan Northwind. Dashboard ini memvisualisasikan data dari PostgreSQL Data Warehouse yang dirancang menggunakan Star Schema.

Dashboard ini mencakup analisis:

📈 Strategi Keuangan (Tren Pendapatan & Kategori)

🌍 Geografi Pasar (Peta Sebaran Global)

💎 Loyalitas Pelanggan (RFM Segmentation & Retention)

📦 Efisiensi Produk (Profitabilitas Item)

💻 Prasyarat
Pastikan perangkat Anda sudah terinstal:

Python (v3.8+)

PostgreSQL (v12+)

DBeaver / pgAdmin (Opsional, untuk manajemen DB visual)

⚙️ Quick Start (Cara Menjalankan)
Ikuti 4 langkah ini untuk menjalankan dashboard di perangkat baru.

1. Import Database
Tidak perlu menjalankan ETL dari awal. Cukup import file northwind_dw.sql yang sudah tersedia.

Cara Import via Terminal:

Bash

# 1. Buat database kosong
createdb -U postgres -h localhost -p 5432 northwind_dw

# 2. Import file SQL
psql -U postgres -h localhost -p 5432 -d northwind_dw < northwind_dw.sql
Cara Import via DBeaver/pgAdmin:

Buat database baru bernama northwind_dw.

Klik kanan database tersebut ➝ Tools ➝ Restore.

Pilih file northwind_dw.sql dari folder proyek ini dan jalankan.

2. Setup Python Environment
Buka terminal di folder proyek, lalu jalankan perintah berikut:

Bash

# 1. Buat Virtual Environment (Disarankan)
python -m venv venv

# 2. Aktifkan Environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 3. Install Library
pip install -r requirements.txt
3. Konfigurasi .env
Buat file baru bernama .env di dalam folder proyek. Isi dengan kredensial PostgreSQL Anda:

Ini, TOML

PG_USER=postgres
PG_PASSWORD=password_postgres_anda
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=northwind_dw
PG_SCHEMA=public
4. Jalankan Dashboard
Jalankan perintah ini di terminal:

Bash

streamlit run app.py
Dashboard akan otomatis terbuka di browser: http://localhost:8501

🔄 Tentang Proses ETL (Opsional)
Folder proyek ini menyertakan script ETL (extract.py, transform.py, load.py) yang digunakan untuk memproses data mentah dari folder data/ ke PostgreSQL.

Anda TIDAK PERLU menjalankan ini jika sudah melakukan langkah Import Database di atas.

Namun, jika Anda ingin membangun ulang Data Warehouse dari data mentah CSV:

Pastikan folder data/ berisi file CSV Northwind yang lengkap.

Jalankan perintah: python etl_main.py

Script akan mengekstrak CSV, melakukan transformasi ke Star Schema, dan memuatnya ke database northwind_dw.

🛠️ Tech Stack
Bahasa: Python

Frontend: Streamlit, Plotly Express

Backend/DW: PostgreSQL, SQLAlchemy

ETL: Pandas (Extract, Transform), Psycopg2 (Load)

Reporting: FPDF (Export PDF), Matplotlib
