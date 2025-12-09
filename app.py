import datetime
import os
import tempfile
import uuid

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import seaborn as sns
import streamlit as st
from dotenv import load_dotenv
from fpdf import FPDF
from sqlalchemy import create_engine

# ==========================================
# 1. KONFIGURASI HALAMAN & SETUP
# ==========================================
st.set_page_config(
    page_title="Northwind Executive Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="📊"
)

# Load environment variables
load_dotenv()

# Styling matplotlib (untuk PDF)
sns.set_theme(style="whitegrid")

# ==========================================
# 2. KONEKSI DATABASE
# ==========================================
@st.cache_resource
def get_dw_engine():
    db_user = os.getenv("PG_USER")
    db_password = os.getenv("PG_PASSWORD")
    db_host = os.getenv("PG_HOST")
    db_port = os.getenv("PG_PORT")
    db_name = os.getenv("PG_DATABASE")
    db_schema = os.getenv("PG_SCHEMA", "public")

    if not all([db_user, db_password, db_host, db_name]):
        st.error("Konfigurasi database di file .env tidak lengkap.")
        return None

    database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        engine = create_engine(
            database_url,
            connect_args={'options': f'-csearch_path={db_schema}'}
        )
        with engine.connect() as conn:
            pass
        return engine
    except Exception as e:
        st.error(f"Gagal terhubung ke database: {e}")
        return None

# ==========================================
# 3. ETL & QUERY DATA
# ==========================================

@st.cache_data(ttl=3600)
def get_dimensions(_engine):
    """Mengambil data dimensi tahun dan kategori"""
    q_date = "SELECT DISTINCT year FROM dim_date ORDER BY year DESC;"
    q_cat = "SELECT DISTINCT category_name FROM dim_product ORDER BY category_name;"
    try:
        df_date = pd.read_sql(q_date, _engine)
        df_cat = pd.read_sql(q_cat, _engine)
        return df_date, df_cat
    except Exception:
        return pd.DataFrame(), pd.DataFrame()

def create_category_filter(selected_categories):
    if selected_categories:
        cats_formatted = "', '".join([c.replace("'", "''") for c in selected_categories])
        return f" AND dp.category_name IN ('{cats_formatted}')"
    return ""

@st.cache_data(ttl=600)
def get_kpi_data(_engine, kpi_type, selected_year, category_sql=""):
    """Mengambil data metrik KPI."""
    
    # 1. KPI FINANCE
    if kpi_type == 'financial_trend':
        query = f"""
        SELECT dd.year, dd.month, dd.month_name, 
               SUM(fs.revenue) AS total_revenue, 
               COUNT(DISTINCT fs.order_id) as total_orders
        FROM fact_sales fs 
        JOIN dim_date dd ON fs.date_key = dd.date_key 
        JOIN dim_product dp ON fs.product_key = dp.product_key 
        WHERE dd.year = {selected_year}
        {category_sql} 
        GROUP BY 1, 2, 3 
        ORDER BY dd.year, dd.month;
        """
        
    # 2. KPI RETENTION
    elif kpi_type == 'retention_rate':
        query = f"""
        WITH customer_monthly AS (
            SELECT 
                dd.year, 
                dd.month,
                fs.customer_key,
                MIN(dd.full_date) as first_purchase_date
            FROM fact_sales fs 
            JOIN dim_date dd ON fs.date_key = dd.date_key 
            JOIN dim_product dp ON fs.product_key = dp.product_key
            WHERE dd.year IN ({selected_year}, {selected_year} - 1)
            {category_sql}
            GROUP BY 1, 2, 3
        ),
        customer_first_purchase AS (
            SELECT 
                customer_key,
                MIN(first_purchase_date) as global_first_purchase
            FROM customer_monthly
            GROUP BY customer_key
        ),
        monthly_metrics AS (
            SELECT 
                cm.year,
                cm.month,
                COUNT(DISTINCT cm.customer_key) as end_customers,
                COUNT(DISTINCT CASE 
                    WHEN EXTRACT(YEAR FROM cfp.global_first_purchase) = cm.year 
                         AND EXTRACT(MONTH FROM cfp.global_first_purchase) = cm.month 
                    THEN cm.customer_key 
                END) as new_customers
            FROM customer_monthly cm
            JOIN customer_first_purchase cfp ON cm.customer_key = cfp.customer_key
            GROUP BY cm.year, cm.month
        ),
        retention_calc AS (
            SELECT 
                year,
                month,
                LAG(end_customers) OVER (ORDER BY year, month) as start_customers,
                end_customers,
                new_customers,
                ROUND(
                    100.0 * (end_customers - new_customers) / 
                    NULLIF(LAG(end_customers) OVER (ORDER BY year, month), 0),
                    2
                ) as retention_rate,
                ROUND(
                    100.0 * (end_customers - LAG(end_customers) OVER (ORDER BY year, month)) / 
                    NULLIF(LAG(end_customers) OVER (ORDER BY year, month), 0),
                    2
                ) as growth_rate,
                ROUND(
                    100.0 - (100.0 * (end_customers - new_customers) / 
                    NULLIF(LAG(end_customers) OVER (ORDER BY year, month), 0)),
                    2
                ) as churn_rate
            FROM monthly_metrics
        )
        SELECT * FROM retention_calc
        WHERE year = {selected_year}
        ORDER BY month;
        """
        
    # 3. KPI CLV
    elif kpi_type == 'customer_clv':
        query = f"""
        SELECT dc.company_name, 
               COUNT(DISTINCT fs.order_id) as frequency, 
               SUM(fs.revenue) as monetary_value,
               SUM(fs.revenue) * 1.2 as predicted_clv 
        FROM fact_sales fs 
        JOIN dim_customer dc ON fs.customer_key = dc.customer_key 
        JOIN dim_date dd ON fs.date_key = dd.date_key 
        JOIN dim_product dp ON fs.product_key = dp.product_key
        WHERE dd.year = {selected_year}
        {category_sql} 
        GROUP BY 1 
        ORDER BY monetary_value DESC;
        """
        
    # 4. KPI PRODUCT
    elif kpi_type == 'product_performance':
        query = f"""
        SELECT dp.product_name, dp.category_name, 
               SUM(fs.revenue) AS total_revenue, 
               SUM(fs.quantity) as total_sold
        FROM fact_sales fs 
        JOIN dim_product dp ON fs.product_key = dp.product_key 
        JOIN dim_date dd ON fs.date_key = dd.date_key
        WHERE dd.year = {selected_year}
        {category_sql} 
        GROUP BY 1, 2 
        ORDER BY total_revenue DESC 
        LIMIT 20;
        """
        
    # 5. KPI CATEGORY
    elif kpi_type == 'category_performance':
        query = f"""
        SELECT dp.category_name, 
               SUM(fs.revenue) AS total_revenue
        FROM fact_sales fs 
        JOIN dim_product dp ON fs.product_key = dp.product_key 
        JOIN dim_date dd ON fs.date_key = dd.date_key
        WHERE dd.year = {selected_year}
        {category_sql} 
        GROUP BY 1 
        ORDER BY total_revenue DESC;
        """

    # 6. KPI GEOGRAPHIC (BARU)
    elif kpi_type == 'geo_performance':
        # Asumsi: kolom country ada di dim_customer
        query = f"""
        SELECT dc.country, 
               SUM(fs.revenue) AS total_revenue,
               COUNT(DISTINCT fs.order_id) as total_orders
        FROM fact_sales fs 
        JOIN dim_customer dc ON fs.customer_key = dc.customer_key
        JOIN dim_date dd ON fs.date_key = dd.date_key
        JOIN dim_product dp ON fs.product_key = dp.product_key
        WHERE dd.year = {selected_year}
        {category_sql} 
        GROUP BY 1 
        ORDER BY total_revenue DESC;
        """
    
    elif kpi_type == 'rfm_raw_data':
        query = f"""
        SELECT 
            dc.company_name as customer_name,
            MAX(dd.full_date) as last_order_date,
            COUNT(DISTINCT fs.order_id) as frequency,
            SUM(fs.revenue) as monetary
        FROM fact_sales fs
        JOIN dim_customer dc ON fs.customer_key = dc.customer_key
        JOIN dim_date dd ON fs.date_key = dd.date_key
        JOIN dim_product dp ON fs.product_key = dp.product_key
        WHERE dd.year = {selected_year}
        {category_sql}
        GROUP BY 1;
        """

    else:
        return pd.DataFrame()

    try:
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Error executing query {kpi_type}: {e}")
        return pd.DataFrame()

# ==========================================
# 4. MODUL INSIGHTS & PDF
# ==========================================

def generate_smart_insights(df_trend, df_retention, df_prod):
    """Insight Generator"""
    insights = {
        "finance": None,
        "customer": None,
        "product": None
    }
    
    if not df_trend.empty:
        peak_row = df_trend.loc[df_trend['total_revenue'].idxmax()]
        low_row = df_trend.loc[df_trend['total_revenue'].idxmin()]
        total_rev = df_trend['total_revenue'].sum()
        
        insights['finance'] = {
            "title": "Kinerja Keuangan",
            "value": f"${total_rev:,.0f}",
            "detail": f"Puncak: **{peak_row['month_name']}** | Terendah: **{low_row['month_name']}**",
            "status": "info"
        }

    if not df_retention.empty:
        avg_retention = df_retention['retention_rate'].mean()
        avg_growth = df_retention['growth_rate'].mean()
        avg_churn = df_retention['churn_rate'].mean()
        
        if avg_retention > 70 and avg_growth > 0:
            status = "success"
            detail = f"Retention Kuat ({avg_retention:.1f}%) & Growth Positif ({avg_growth:.1f}%)"
        elif avg_retention > 50:
            status = "info"
            detail = f"Retention Sedang ({avg_retention:.1f}%) | Churn: {avg_churn:.1f}%"
        else:
            status = "error"
            detail = f"Retention Rendah ({avg_retention:.1f}%) | Churn Tinggi: {avg_churn:.1f}%"
        
        insights['customer'] = {
            "title": "Kesehatan Pelanggan",
            "value": f"{avg_retention:.1f}%",
            "detail": detail,
            "status": status
        }

    if not df_prod.empty:
        top_prod = df_prod.iloc[0]
        top_cat_name = top_prod['category_name']
        
        insights['product'] = {
            "title": "Produk Unggulan",
            "value": f"{top_prod['product_name']}",
            "detail": f"Rev: **${top_prod['total_revenue']:,.0f}** | Kat: **{top_cat_name}**",
            "status": "success"
        }

    return insights


def process_rfm_segmentation(df_rfm, analysis_date):
    if df_rfm.empty:
        return df_rfm
    
    df_rfm['last_order_date'] = pd.to_datetime(df_rfm['last_order_date'])
    df_rfm['recency'] = (analysis_date - df_rfm['last_order_date']).dt.days
    
    # Scoring 1-5
    df_rfm['R_Score'] = pd.qcut(df_rfm['recency'].rank(method='first'), q=5, labels=[5, 4, 3, 2, 1])
    df_rfm['F_Score'] = pd.qcut(df_rfm['frequency'].rank(method='first'), q=5, labels=[1, 2, 3, 4, 5])
    
    # Mapping ke 5 Segmen Utama
    seg_map = {
        r'[1-2][1-2]': 'Lost',            # Sudah lama pergi, jarang beli
        r'[1-2][3-5]': 'At Risk',         # Dulu sering beli, sekarang menghilang
        r'[3-4]1':     'New Customers',   # Baru beli sekali/dua kali
        r'51':         'New Customers',   # Baru banget beli
        r'[3-4][2-3]': 'Potential',       # Rajin tapi belum "Wow"
        r'[3-4][4-5]': 'Loyal Customers', # Konsisten
        r'5[2-3]':     'Loyal Customers', # Konsisten & Baru beli
        r'5[4-5]':     'Champions'        # Terbaik (Baru beli & Sering banget)
    }
    
    df_rfm['Segment'] = (df_rfm['R_Score'].astype(str) + df_rfm['F_Score'].astype(str)).replace(seg_map, regex=True)
    
    # Fallback safety net
    valid_segments = ['Champions', 'Loyal Customers', 'Potential', 'New Customers', 'At Risk', 'Lost']
    df_rfm.loc[~df_rfm['Segment'].isin(valid_segments), 'Segment'] = 'Potential'
    
    return df_rfm


class PDFReport(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 10)
        self.set_text_color(150)
        self.cell(0, 10, 'KELOMPOK 4 - STRATEGIC REPORT', 0, 1, 'R')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(128)
        self.cell(0, 10, f'Halaman {self.page_no()}', 0, 0, 'C')

    def chapter_title(self, label):
        self.set_font('Arial', 'B', 16)
        self.set_text_color(31, 119, 180) 
        self.cell(0, 10, label, 0, 1, 'L')
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(5)

    def chapter_body(self, text):
        self.set_font('Arial', '', 11)
        self.set_text_color(50)
        self.multi_cell(0, 6, text)
        self.ln()

    def add_metric_box(self, label, value, x_pos, y_pos, width=45):
        self.set_xy(x_pos, y_pos)
        self.set_fill_color(240, 242, 246) 
        self.rect(x_pos, y_pos, width, 25, 'F')
        
        self.set_xy(x_pos, y_pos + 3)
        self.set_font('Arial', '', 9)
        self.set_text_color(100)
        self.cell(width, 5, label, 0, 2, 'C')
        
        self.set_font('Arial', 'B', 14)
        self.set_text_color(0)
        self.cell(width, 8, str(value), 0, 2, 'C')

    def create_table(self, df, col_widths, col_names):
        self.set_font('Arial', 'B', 10)
        self.set_fill_color(31, 119, 180)
        self.set_text_color(255)
        
        for col_name, width in zip(col_names, col_widths):
            self.cell(width, 8, col_name, 1, 0, 'C', True)
        self.ln()
        
        self.set_font('Arial', '', 9)
        self.set_text_color(0)
        fill = False
        for _, row in df.iterrows():
            for item, width in zip(row, col_widths):
                text = str(item)
                if len(text) > 25: text = text[:22] + "..." 
                self.cell(width, 7, text, 1, 0, 'L', fill)
            self.ln()
            fill = not fill 

def save_plot_to_image(fig):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmpfile:
        fig.savefig(tmpfile.name, bbox_inches='tight', dpi=100)
        return tmpfile.name

def generate_pdf(data_dict, year_label):
    pdf = PDFReport()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    df_fin = data_dict.get('financial', pd.DataFrame())
    df_cust = data_dict.get('retention', pd.DataFrame())
    df_prod = data_dict.get('product', pd.DataFrame())
    df_clv = data_dict.get('clv', pd.DataFrame())

    # HALAMAN 1
    pdf.add_page()
    pdf.set_font('Arial', 'B', 20)
    pdf.cell(0, 15, f'Laporan Strategis Northwind: {year_label}', 0, 1, 'C')
    pdf.set_font('Arial', 'I', 10)
    pdf.cell(0, 5, f'Tanggal Cetak: {datetime.datetime.now().strftime("%d %B %Y")}', 0, 1, 'C')
    pdf.ln(10)

    start_y = pdf.get_y()
    
    total_rev = df_fin['total_revenue'].sum() if not df_fin.empty else 0
    avg_ret = df_cust['retention_rate'].mean() if not df_cust.empty else 0
    top_prod_name = df_prod.iloc[0]['product_name'] if not df_prod.empty else "-"
    if len(top_prod_name) > 15: top_prod_name = top_prod_name[:12] + "..."

    # Layout PDF Manual
    pdf.add_metric_box("Total Revenue", f"${total_rev:,.0f}", 15, start_y)
    pdf.add_metric_box("Avg Retention", f"{avg_ret:.1f}%", 65, start_y)
    pdf.add_metric_box("Top Product", top_prod_name, 115, start_y)
    pdf.add_metric_box("Active Month", f"{len(df_fin)} Bulan", 165, start_y)
    
    pdf.set_y(start_y + 35)

    # 1. KEUANGAN
    pdf.chapter_title('1. Analisis Keuangan')
    if not df_fin.empty:
        pdf.chapter_body(f"Total pendapatan tahun ini mencapai ${total_rev:,.0f}. Grafik di bawah menunjukkan tren bulanan.")
        
        plt.figure(figsize=(10, 4))
        sns.lineplot(data=df_fin, x='month_name', y='total_revenue', marker='o', linewidth=2.5, color='#1f77b4')
        plt.title(f'Tren Pendapatan Bulanan - {year_label}')
        plt.ylabel('Revenue ($)')
        plt.xlabel('')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(rotation=45)
        
        img_path = save_plot_to_image(plt.gcf())
        pdf.image(img_path, x=10, w=190)
        plt.close()
        os.unlink(img_path)
    else:
        pdf.chapter_body("Data keuangan tidak tersedia.")

    # 2. CUSTOMER
    pdf.add_page()
    pdf.chapter_title('2. Kesehatan & Retensi Pelanggan')
    
    if not df_cust.empty:
        plt.figure(figsize=(10, 4))
        plt.plot(df_cust['month'], df_cust['retention_rate'], label='Retention %', color='green', marker='o')
        plt.plot(df_cust['month'], df_cust['churn_rate'], label='Churn %', color='red', marker='x')
        plt.title('Retention vs Churn Rate')
        plt.legend()
        plt.grid(True, alpha=0.5)
        
        img_path = save_plot_to_image(plt.gcf())
        pdf.image(img_path, x=10, w=190)
        plt.close()
        os.unlink(img_path)
        pdf.ln(5)

    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'Top 5 Pelanggan Berdasarkan Nilai Transaksi', 0, 1)
    
    if not df_clv.empty:
        top_clv = df_clv.head(5).copy()
        top_clv['formatted_val'] = top_clv['monetary_value'].apply(lambda x: f"${x:,.0f}")
        table_data = top_clv[['company_name', 'frequency', 'formatted_val']]
        
        pdf.create_table(
            table_data, 
            col_widths=[100, 30, 60], 
            col_names=['Nama Perusahaan', 'Order', 'Total Belanja']
        )
    pdf.ln(10)

    # 3. PRODUK
    pdf.add_page()
    pdf.chapter_title('3. Performa Produk')

    if not df_prod.empty:
        top_10 = df_prod.head(10).sort_values('total_revenue', ascending=True)
        
        plt.figure(figsize=(10, 6))
        bars = plt.barh(top_10['product_name'], top_10['total_revenue'], color='#1f77b4')
        plt.title('Top 10 Produk (Revenue)')
        plt.xlabel('Revenue ($)')
        
        img_path = save_plot_to_image(plt.gcf())
        pdf.image(img_path, x=10, w=180)
        plt.close()
        os.unlink(img_path)
        
        pdf.ln(5)
        
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 10, 'Detail Top 5 Produk', 0, 1)
        
        top_5_prod = df_prod.head(5).copy()
        top_5_prod['rev_fmt'] = top_5_prod['total_revenue'].apply(lambda x: f"${x:,.0f}")
        
        pdf.create_table(
            top_5_prod[['product_name', 'category_name', 'total_sold', 'rev_fmt']],
            col_widths=[70, 40, 30, 50],
            col_names=['Nama Produk', 'Kategori', 'Qty', 'Revenue']
        )

    return pdf.output(dest='S').encode('latin-1')

# ==========================================
# 5. DASHBOARD UTAMA
# ==========================================
def main():
    engine = get_dw_engine()
    if not engine:
        st.stop()

    # --- SIDEBAR FILTER ---
    st.sidebar.header("🎛️ Filter Dashboard")
    df_date, df_cat = get_dimensions(engine)
    
    # 1. FILTER TAHUN
    if not df_date.empty:
        available_years = sorted(df_date['year'].unique().tolist(), reverse=True)
        sel_year = st.sidebar.selectbox("Pilih Tahun:", available_years, index=0)
    else:
        st.sidebar.warning("Data tahun tidak ditemukan.")
        st.stop()

    # 2. FILTER KATEGORI
    if not df_cat.empty:
        opt_cat = df_cat['category_name'].unique().tolist()
        sel_cat = st.sidebar.multiselect("Filter Kategori:", opt_cat, default=opt_cat)
    else:
        sel_cat = []
        
    if not sel_cat:
        st.warning("Mohon pilih minimal satu kategori produk.")
        st.stop()
        
    category_sql = create_category_filter(sel_cat)

    # --- MAIN CONTENT ---
    st.title("🚀 Northwind Strategic Dashboard")
    st.caption(f"Tahun Analisis: {sel_year} ")
    
    # Fetch Data
    with st.spinner('Menghitung metrik KPI...'):
        df_trend = get_kpi_data(engine, 'financial_trend', sel_year, category_sql)
        df_retention = get_kpi_data(engine, 'retention_rate', sel_year, category_sql)
        df_clv = get_kpi_data(engine, 'customer_clv', sel_year, category_sql)
        df_prod = get_kpi_data(engine, 'product_performance', sel_year, category_sql)
        df_cat_perf = get_kpi_data(engine, 'category_performance', sel_year, category_sql)
        df_geo = get_kpi_data(engine, 'geo_performance', sel_year, category_sql)
        df_rfm_raw = get_kpi_data(engine, 'rfm_raw_data', sel_year, category_sql)
        analysis_date = pd.Timestamp(year=sel_year, month=12, day=31)
        df_rfm_segmented = process_rfm_segmentation(df_rfm_raw, analysis_date)
        df_retention = get_kpi_data(engine, 'retention_rate', sel_year, category_sql)
        # 2. [TAMBAHAN] Ambil Data Retention Tahun Lalu untuk Perbandingan
        df_retention_prev = get_kpi_data(engine, 'retention_rate', sel_year - 1, category_sql)

    # --- KPI SCORECARDS (UPDATED WITH AOV) ---
    col1, col2, col3, col4 = st.columns(4) # <--- Sekarang ada 5 Kolom
    
    total_revenue = df_trend['total_revenue'].sum() if not df_trend.empty else 0
    
    avg_retention = df_retention['retention_rate'].mean() if not df_retention.empty else 0
    active_customers = len(df_clv) if not df_clv.empty else 0
    top_product = df_prod.iloc[0]['product_name'] if not df_prod.empty else "-"
    avg_retention = df_retention['retention_rate'].mean() if not df_retention.empty else 0
    # Rata-rata tahun lalu
    avg_retention_prev = df_retention_prev['retention_rate'].mean() if not df_retention_prev.empty else 0
    # Hitung selisih (Delta)
    retention_delta = avg_retention - avg_retention_prev

    
    col1.metric("💰 Total Revenue", f"${total_revenue:,.0f}", delta="Finance")
    col2.metric(
        "🔄 Avg Retention", 
        f"{avg_retention:.1f}%", 
        delta=f"{retention_delta:+.1f}% vs Last Year" # Menampilkan selisih dengan tahun lalu
    )
    col3.metric("👥 Active Customers", f"{active_customers}", delta="Base")
    col4.metric("🏆 Top Product", top_product[:15]+"..." if len(top_product)>15 else top_product, delta="Leader")
    
    # --- AUTOMATED INSIGHTS ---
    st.markdown("---")
    ins = generate_smart_insights(df_trend, df_retention, df_prod)
    
    with st.expander("💡 SUMMARY & AUTOMATED INSIGHTS", expanded=True):
        c1, c2, c3 = st.columns(3)
        
        with c1:
            if ins['finance']:
                st.info(f"**{ins['finance']['title']}**\n\n"
                        f"### {ins['finance']['value']}\n"
                        f"{ins['finance']['detail']}")
        
        with c2:
            if ins['customer']:
                content = (f"**{ins['customer']['title']}**\n\n"
                           f"### {ins['customer']['value']}\n"
                           f"{ins['customer']['detail']}")
                if ins['customer']['status'] == 'error':
                    st.error(content, icon="📉")
                elif ins['customer']['status'] == 'success':
                    st.success(content, icon="✅")
                else:
                    st.info(content, icon="ℹ️")

        with c3:
            if ins['product']:
                prod_name = ins['product']['value']
                if len(prod_name) > 25: prod_name = prod_name[:25] + "..."
                st.success(f"**{ins['product']['title']}**\n\n"
                           f"### {prod_name}\n"
                           f"{ins['product']['detail']}", icon="🏆")

    # --- TABS ANALYSIS ---
    # Menambahkan Tab baru untuk Peta
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Strategi Keuangan", "🌍 Geografi Pasar", "🤝 Loyalitas Pelanggan", "📦 Efisiensi Produk"])

    # TAB 1: FINANCIAL
    with tab1:
        st.subheader(f"Tren Pendapatan Tahun {sel_year}")
        col_a, col_b = st.columns([2, 1])
        
        with col_a:
            if not df_trend.empty:
                df_trend['period'] = df_trend['year'].astype(str) + '-' + df_trend['month'].astype(str).str.zfill(2)
                
                fig_trend = px.area(
                    df_trend,
                    x='period',
                    y='total_revenue',
                    title='Arus Kas Bulanan',
                    labels={'period': 'Bulan', 'total_revenue': 'Revenue ($)'},
                    markers=True
                )
                fig_trend.update_traces(line_color='#1f77b4', fillcolor='rgba(31, 119, 180, 0.3)')
                st.plotly_chart(fig_trend, use_container_width=True)
                
        with col_b:
            if not df_cat_perf.empty:
                fig_pie = px.pie(
                    df_cat_perf,
                    names='category_name',
                    values='total_revenue',
                    title='Proporsi Kategori',
                    hole=0.4
                )
                fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_pie, use_container_width=True)

    # TAB 2: GEOGRAPHIC (NEW!)
    with tab2:
        st.subheader("Sebaran Penjualan Global")
        st.markdown("Analisis pasar berdasarkan lokasi pelanggan untuk mengidentifikasi wilayah potensial.")
        
        if not df_geo.empty:
            # 1. Peta Choropleth
            fig_map = px.choropleth(
                df_geo,
                locations="country",
                locationmode="country names",
                color="total_revenue",
                hover_name="country",
                hover_data={"total_orders": True, "total_revenue": ":$.2f"},
                color_continuous_scale="Viridis", 
                title=f"Distribusi Revenue per Negara ({sel_year})"
            )
            fig_map.update_geos(projection_type="natural earth")
            fig_map.update_layout(height=500, margin={"r":0,"t":50,"l":0,"b":0})
            st.plotly_chart(fig_map, use_container_width=True)
            
            # 2. Tabel Detail Negara
            with st.expander("🌍 Lihat Data Detail Per Negara"):
                st.dataframe(
                    df_geo.style.format({'total_revenue': '${:,.2f}'})
                    .background_gradient(cmap='Blues', subset=['total_revenue']),
                    use_container_width=True
                )
        else:
            st.warning("Data geografis tidak tersedia.")

# TAB 3: LOYALITAS (RFM SEGMENTATION - UPDATED)
    with tab3:
        # 1. Retention & Growth (Bagian Atas)
        st.subheader("Analisis Customer Retention")
        st.info("""
        ℹ️ **Formula Retention Rate:** `((Pelanggan Akhir - Pelanggan Baru) / Pelanggan Awal) × 100%`
        
        - **Retention Rate**: Persentase pelanggan lama yang tetap aktif
        - **Churn Rate**: Persentase pelanggan yang hilang (100% - Retention)
        """)
        
        col_c, col_d = st.columns(2)
        
        with col_c:
            st.markdown("##### 📊 Trend Retention Metrics")
            if not df_retention.empty:
                df_retention['period'] = df_retention['year'].astype(str) + '-' + df_retention['month'].astype(str).str.zfill(2)
                
                df_plot = df_retention.melt(
                    id_vars=['period', 'month'],
                    value_vars=['retention_rate', 'churn_rate'],
                    var_name='Metric',
                    value_name='Percentage'
                )
                
                retention_colors = {'retention_rate': '#2ecc71', 'churn_rate': '#e74c3c'}
                
                fig_metrics = px.line(
                    df_plot, x='period', y='Percentage', color='Metric',
                    title='Metrik Kesehatan Pelanggan', markers=True,
                    color_discrete_map=retention_colors
                )
                st.plotly_chart(fig_metrics, use_container_width=True)
                
        with col_d:
            st.markdown("##### 💎 Top 10 High Value Customers")
            if not df_clv.empty:
                st.dataframe(
                    df_clv.head(10)[['company_name', 'frequency', 'monetary_value']]
                    .rename(columns={'company_name': 'Customer', 'monetary_value': 'Total Spend', 'frequency': 'Orders'})
                    .style.format({'Total Spend': '${:,.0f}'})
                    .background_gradient(cmap='Greens', subset=['Total Spend']),
                    use_container_width=True,
                    hide_index=True
                )

        # 2. RFM Analysis (Bagian Bawah - 5 Segmen)
        st.markdown("---")
        st.subheader("Segmentasi Pelanggan")
        
        # --- [TAMBAHAN] PENJELASAN SEGMEN ---
        with st.expander("ℹ️ Penjelasan Kategori Pelanggan (Klik untuk Membuka)"):
            st.markdown("""
            Pelanggan dikelompokkan berdasarkan **Recency** (hari sejak belanja terakhir) dan **Frequency** (jumlah transaksi):
            
            1.  🥇 **Champions:** Pelanggan terbaik! Baru saja berbelanja (Recency kecil) dan sangat sering bertransaksi.
            2.  💎 **Loyal Customers:** Pelanggan setia yang berbelanja secara konsisten.
            3.  🌱 **Potential / New:** Pelanggan baru atau pelanggan yang berpotensi menjadi loyal jika dirawat.
            4.  ⚠️ **At Risk:** Pelanggan yang dulunya sering belanja, tapi **sudah lama menghilang**. Perlu segera dihubungi!
            5.  💤 **Lost:** Pelanggan yang sudah lama tidak belanja dan frekuensi belanjanya rendah.
            """)
        
        if not df_rfm_segmented.empty:
            col_rfm1, col_rfm2 = st.columns(2)
            
            rfm_colors = {
                'Champions': '#198754', 'Loyal Customers': '#0dcaf0',
                'Potential': '#ffc107', 'New Customers': '#6f42c1',
                'At Risk': '#fd7e14', 'Lost': '#dc3545'
            }
            
            with col_rfm1:
                # Horizontal Bar Chart (Avg Spend)
                rfm_avg_monetary = df_rfm_segmented.groupby('Segment')['monetary'].mean().reset_index()
                rfm_avg_monetary.columns = ['Segment', 'Avg Spend']

                fig_bar_avg = px.bar(
                    rfm_avg_monetary, x='Avg Spend', y='Segment', orientation='h',
                    color='Segment', color_discrete_map=rfm_colors,
                    title='Rata-rata Nilai Transaksi per User ($)',
                    text_auto='.2s'
                )
                fig_bar_avg.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False)
                st.plotly_chart(fig_bar_avg, use_container_width=True)
                
            with col_rfm2:
                # Scatter Plot
                fig_scatter = px.scatter(
                    df_rfm_segmented, x='recency', y='monetary', color='Segment',
                    size='frequency', hover_name='customer_name',
                    title='Peta Persebaran: Recency vs Monetary',
                    color_discrete_map=rfm_colors
                )
                st.plotly_chart(fig_scatter, use_container_width=True)
            
            st.markdown("### 📋 Daftar Target Pelanggan per Segmen")
            
            avail_seg = [s for s in list(rfm_colors.keys()) if s in df_rfm_segmented['Segment'].unique()]
            sel_seg = st.selectbox("Filter Segmen:", avail_seg)
            
            data_show = df_rfm_segmented[df_rfm_segmented['Segment'] == sel_seg]
            
            # --- [ADJUSTMENT] TABEL DENGAN WARNA PADA RECENCY ---
            st.dataframe(
                data_show[['customer_name', 'last_order_date', 'recency', 'frequency', 'monetary']]
                .sort_values('monetary', ascending=False)
                .style.format({'monetary': '${:,.0f}'})
                # cmap='Reds': Semakin tinggi recency (semakin lama tidak belanja), warna semakin merah gelap
                .background_gradient(cmap='Reds', subset=['recency']),
                use_container_width=True
            )

    # TAB 4: PRODUCT
    with tab4:
        st.subheader("Profitabilitas Produk")
        if not df_prod.empty:
            fig_prod = px.bar(
                df_prod.head(15),
                x='total_revenue',
                y='product_name',
                orientation='h',
                title='Top 15 Produk Paling Menguntungkan',
                color='category_name',
                labels={'total_revenue': 'Revenue ($)', 'product_name': 'Nama Produk'}
            )
            fig_prod.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_prod, use_container_width=True)

    # --- DOWNLOAD BUTTON ---
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🖨️ Ekspor Data")
    if st.sidebar.button("📥 Unduh Laporan PDF"):
        with st.spinner("Membuat Laporan Lengkap..."):
            data_export = {
                'financial': df_trend,
                'retention': df_retention,
                'clv': df_clv,
                'product': df_prod
            }
            pdf_bytes = generate_pdf(data_export, str(sel_year))
            st.sidebar.download_button(
                label="Klik untuk Download PDF",
                data=pdf_bytes,
                file_name=f"Northwind_Report_{sel_year}.pdf",
                mime="application/pdf"
            )

if __name__ == "__main__":
    main()