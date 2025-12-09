🚀 Northwind Strategic Dashboard
Dashboard Business Intelligence interaktif berbasis Streamlit untuk menganalisis kinerja perusahaan Northwind. Dashboard ini mencakup analisis Strategi Keuangan, Geografi Pasar, Segmentasi Pelanggan (RFM Analysis), dan Efisiensi Produk yang terhubung langsung dengan PostgreSQL Data Warehouse.

📋 Daftar Isi
Prasyarat Sistem

Struktur Proyek

Migrasi Database (Export & Import)

Instalasi & Setup Environment

Konfigurasi .env

Menjalankan Dashboard

💻 Prasyarat Sistem
Sebelum memulai, pastikan perangkat target sudah terinstal:

DBeaver (Opsional, untuk manajemen database visual)

Git (Opsional, untuk kloning repository)

📂 Struktur Proyek
Pastikan folder proyek Anda memiliki susunan file seperti ini:

Plaintext

northwind-dashboard/
│
├── app.py                 # File utama aplikasi Streamlit (Source code dashboard)
├── .env                   # File konfigurasi kredensial database (JANGAN di-upload ke GitHub)
├── requirements.txt       # Daftar library Python yang dibutuhkan
├── northwind_dw.sql       # File backup database (hasil export)
└── README.md              # Dokumentasi proyek ini
(Catatan: Jika Anda memisahkan koneksi database, pastikan file db_connection.py ada dalam folder yang sama).

📦 Migrasi Database (Export & Import)
Agar dashboard berjalan di perangkat baru, Anda harus memindahkan Data Warehouse (PostgreSQL).

Langkah 1: Export Database (Di Laptop Lama/Sumber)
Jika Anda menggunakan DBeaver atau pgAdmin, Anda bisa klik kanan pada database -> Backup/Export. Jika menggunakan Command Line (CMD/Terminal):

Bash

# Format: pg_dump -U [username] -h [host] -p [port] -d [nama_database] > [nama_file_keluaran.sql]
pg_dump -U postgres -h localhost -p 5432 -d northwind_dw > northwind_dw.sql
File northwind_dw.sql akan muncul di folder Anda. Salin file ini ke laptop baru.

Langkah 2: Import Database (Di Laptop Baru/Target)
Buka PostgreSQL (via pgAdmin/DBeaver) di laptop baru.

Buat database kosong baru, misal bernama northwind_dw.

Restore/Import file SQL tadi.

Jika menggunakan Command Line:

Bash

# 1. Buat database baru (jika belum ada)
createdb -U postgres -h localhost -p 5432 northwind_dw

# 2. Import data dari file SQL
psql -U postgres -h localhost -p 5432 -d northwind_dw < northwind_dw.sql
⚙️ Instalasi & Setup Environment
Buka Terminal (CMD/PowerShell/VS Code Terminal) di dalam folder proyek.

Buat Virtual Environment (Sangat disarankan agar library tidak bentrok):

Bash

# Untuk Windows
python -m venv venv
venv\Scripts\activate

# Untuk Mac/Linux
python3 -m venv venv
source venv/bin/activate
Install Library yang Dibutuhkan: Buat file requirements.txt (jika belum ada) dengan isi berikut:

Plaintext

streamlit
pandas
sqlalchemy
psycopg2-binary
plotly
matplotlib
seaborn
python-dotenv
fpdf
Lalu jalankan perintah:

Bash

pip install -r requirements.txt
🔑 Konfigurasi .env
Buat file baru bernama .env di dalam folder proyek. File ini berfungsi menyimpan rahasia koneksi database agar tidak tertulis langsung di dalam kode (app.py).

Isi file .env dengan format berikut (sesuaikan dengan setting PostgreSQL di laptop baru):

Cuplikan kode

# Konfigurasi Database PostgreSQL
PG_USER=postgres
PG_PASSWORD=password_postgres_anda
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=northwind_dw
PG_SCHEMA=public
PG_USER: Biasanya postgres (default).

PG_PASSWORD: Password yang Anda buat saat instalasi PostgreSQL.

PG_DATABASE: Nama database yang tadi Anda buat saat proses Import (Langkah 3).

🚀 Menjalankan Dashboard
Setelah database siap dan library terinstall:

Pastikan virtual environment aktif.

Jalankan perintah berikut di terminal:

Bash

streamlit run app.py
Browser akan otomatis terbuka di alamat http://localhost:8501. Dashboard siap digunakan! 🎉