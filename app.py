import datetime
import os
import tempfile
import uuid

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

load_dotenv()
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

    elif kpi_type == 'geo_performance':
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
# 4. FUNGSI TARGET & CHART HELPER
# ==========================================

def calculate_achievement(actual, target):
    if target == 0: return 0
    return round((actual / target) * 100, 1)

def create_revenue_comparison_chart(df_trend, monthly_target):
    df_trend['target'] = monthly_target
    
    fig = go.Figure()
    
    # Bar Actual Revenue
    fig.add_trace(go.Bar(
        x=df_trend['month_name'],
        y=df_trend['total_revenue'],
        name='Actual Revenue',
        marker_color='#1f77b4',
        text=df_trend['total_revenue'].apply(lambda x: f'${x:,.0f}'),
        textposition='outside'
    ))
    
    # Line Target
    fig.add_trace(go.Scatter(
        x=df_trend['month_name'],
        y=df_trend['target'],
        name='Target',
        mode='lines+markers',
        line=dict(color='red', width=3, dash='dash'),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        title='Revenue: Actual vs Target',
        xaxis_title='Month',
        yaxis_title='Revenue ($)',
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig

def create_retention_comparison_chart(df_retention, target_retention):
    df_retention['target'] = target_retention
    df_retention['gap'] = df_retention['retention_rate'] - target_retention
    
    fig = go.Figure()
    
    # Area Actual Retention
    fig.add_trace(go.Scatter(
        x=df_retention['month'],
        y=df_retention['retention_rate'],
        name='Actual Retention',
        fill='tozeroy',
        mode='lines+markers',
        line=dict(color='#2ecc71', width=3),
        marker=dict(size=8)
    ))
    
    # Line Target
    fig.add_trace(go.Scatter(
        x=df_retention['month'],
        y=df_retention['target'],
        name='Target Retention',
        mode='lines',
        line=dict(color='red', width=3, dash='dash')
    ))
    
    fig.update_layout(
        title='Retention Rate: Actual vs Target',
        xaxis_title='Month',
        yaxis_title='Retention Rate (%)',
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig, df_retention

# ==========================================
# 5. MODUL INSIGHTS & RFM
# ==========================================

def generate_smart_insights(df_trend, df_retention, df_prod):
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
            detail = f"Retention Kuat ({avg_retention:.1f}%)"
        elif avg_retention > 50:
            status = "info"
            detail = f"Retention Sedang ({avg_retention:.1f}%)"
        else:
            status = "error"
            detail = f"Retention Rendah ({avg_retention:.1f}%)"
        
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
    
    df_rfm['R_Score'] = pd.qcut(df_rfm['recency'].rank(method='first'), q=5, labels=[5, 4, 3, 2, 1])
    df_rfm['F_Score'] = pd.qcut(df_rfm['frequency'].rank(method='first'), q=5, labels=[1, 2, 3, 4, 5])
    
    seg_map = {
        r'[1-2][1-2]': 'Lost',
        r'[1-2][3-5]': 'At Risk',
        r'[3-4]1': 'New Customers',
        r'51': 'New Customers',
        r'[3-4][2-3]': 'Potential',
        r'[3-4][4-5]': 'Loyal Customers',
        r'5[2-3]': 'Loyal Customers',
        r'5[4-5]': 'Champions'
    }
    
    df_rfm['Segment'] = (df_rfm['R_Score'].astype(str) + df_rfm['F_Score'].astype(str)).replace(seg_map, regex=True)
    
    valid_segments = ['Champions', 'Loyal Customers', 'Potential', 'New Customers', 'At Risk', 'Lost']
    df_rfm.loc[~df_rfm['Segment'].isin(valid_segments), 'Segment'] = 'Potential'
    
    return df_rfm

# ==========================================
# 6. PDF GENERATION LOGIC (UPDATED WITH TARGETS)
# ==========================================

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

def generate_pdf(data_dict, year_label, rev_target, ret_target):
    pdf = PDFReport()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    df_fin = data_dict.get('financial', pd.DataFrame())
    df_cust = data_dict.get('retention', pd.DataFrame())
    df_prod = data_dict.get('product', pd.DataFrame())
    df_clv = data_dict.get('clv', pd.DataFrame())

    # HALAMAN 1: Header
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

    # Scorecards
    pdf.add_metric_box("Total Revenue", f"${total_rev:,.0f}", 15, start_y)
    pdf.add_metric_box("Avg Retention", f"{avg_ret:.1f}%", 65, start_y)
    pdf.add_metric_box("Top Product", top_prod_name, 115, start_y)
    pdf.add_metric_box("Active Month", f"{len(df_fin)} Bulan", 165, start_y)
    
    pdf.set_y(start_y + 35)

    # 1. KEUANGAN (Revenue vs Target)
    pdf.chapter_title('1. Pencapaian Target Keuangan')
    if not df_fin.empty:
        pdf.chapter_body(f"Total revenue tahun ini mencapai ${total_rev:,.0f} dengan target bulanan ${rev_target:,.0f}.")
        
        # CHART: Revenue Bar + Target Line (Matplotlib)
        plt.figure(figsize=(10, 4))
        sns.barplot(data=df_fin, x='month_name', y='total_revenue', color='#1f77b4', label='Actual')
        plt.axhline(y=rev_target, color='red', linestyle='--', linewidth=2, label=f'Target (${rev_target:,.0f})')
        plt.title(f'Monthly Revenue vs Target ({year_label})')
        plt.ylabel('Revenue ($)')
        plt.xlabel('')
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.5)
        plt.xticks(rotation=45)
        
        img_path = save_plot_to_image(plt.gcf())
        pdf.image(img_path, x=10, w=190)
        plt.close()
        os.unlink(img_path)
        
        pdf.ln(5)
        
        # TABLE: Revenue Summary
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 10, 'Rincian Pencapaian Revenue Bulanan', 0, 1)
        
        # Prepare table data
        df_fin['Target'] = rev_target
        df_fin['Achv'] = (df_fin['total_revenue'] / rev_target) * 100
        
        table_data = df_fin[['month_name', 'total_revenue', 'Target', 'Achv']].copy()
        # Formatting data for PDF table
        table_data['total_revenue'] = table_data['total_revenue'].apply(lambda x: f"${x:,.0f}")
        table_data['Target'] = table_data['Target'].apply(lambda x: f"${x:,.0f}")
        table_data['Achv'] = table_data['Achv'].apply(lambda x: f"{x:.1f}%")
        
        pdf.create_table(
            table_data, 
            col_widths=[50, 45, 45, 40], 
            col_names=['Bulan', 'Actual', 'Target', 'Achievement']
        )
    else:
        pdf.chapter_body("Data keuangan tidak tersedia.")

    # 2. CUSTOMER (Retention vs Target)
    pdf.add_page()
    pdf.chapter_title('2. Target Retensi Pelanggan')
    
    if not df_cust.empty:
        pdf.chapter_body(f"Rata-rata retensi adalah {avg_ret:.1f}% dibandingkan target {ret_target}%.")

        # CHART: Retention Line + Target Line
        plt.figure(figsize=(10, 4))
        plt.plot(df_cust['month'], df_cust['retention_rate'], marker='o', color='green', linewidth=2, label='Actual %')
        plt.axhline(y=ret_target, color='red', linestyle='--', linewidth=2, label=f'Target ({ret_target}%)')
        plt.title('Monthly Retention Rate vs Target')
        plt.ylim(0, 150)
        plt.ylabel('Retention Rate (%)')
        plt.legend()
        plt.grid(True, alpha=0.5)
        
        img_path = save_plot_to_image(plt.gcf())
        pdf.image(img_path, x=10, w=190)
        plt.close()
        os.unlink(img_path)
        pdf.ln(5)

        # TABLE: Retention Summary
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 10, 'Analisis Gap Retensi', 0, 1)
        
        df_cust['Target_Ret'] = ret_target
        df_cust['Gap'] = df_cust['retention_rate'] - ret_target
        
        table_data_cust = df_cust[['month', 'retention_rate', 'Target_Ret', 'Gap']].copy()
        table_data_cust['retention_rate'] = table_data_cust['retention_rate'].apply(lambda x: f"{x:.1f}%")
        table_data_cust['Target_Ret'] = table_data_cust['Target_Ret'].apply(lambda x: f"{x}%")
        table_data_cust['Gap'] = table_data_cust['Gap'].apply(lambda x: f"{x:+.1f}%")
        
        pdf.create_table(
            table_data_cust,
            col_widths=[30, 50, 50, 50],
            col_names=['Bulan', 'Actual Rate', 'Target Rate', 'Gap vs Target']
        )
    pdf.ln(10)

    # 3. PRODUK & CLV (Tidak ada perubahan signifikan, tetap rapi)
    pdf.add_page()
    pdf.chapter_title('3. Top Produk & Pelanggan')

    if not df_prod.empty:
        # Chart Product
        top_10 = df_prod.head(10).sort_values('total_revenue', ascending=True)
        plt.figure(figsize=(10, 5))
        bars = plt.barh(top_10['product_name'], top_10['total_revenue'], color='#1f77b4')
        plt.title('Top 10 Produk (Revenue)')
        plt.xlabel('Revenue ($)')
        img_path = save_plot_to_image(plt.gcf())
        pdf.image(img_path, x=10, w=180)
        plt.close()
        os.unlink(img_path)
        pdf.ln(5)
        
    if not df_clv.empty:
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 10, 'Top 5 Pelanggan (High Value)', 0, 1)
        
        top_clv = df_clv.head(5).copy()
        top_clv['formatted_val'] = top_clv['monetary_value'].apply(lambda x: f"${x:,.0f}")
        table_data = top_clv[['company_name', 'frequency', 'formatted_val']]
        
        pdf.create_table(
            table_data, 
            col_widths=[100, 30, 50], 
            col_names=['Nama Perusahaan', 'Order', 'Total Belanja']
        )

    return pdf.output(dest='S').encode('latin-1')

# ==========================================
# 7. DASHBOARD UTAMA
# ==========================================
def main():
    engine = get_dw_engine()
    if not engine:
        st.stop()

    # --- SIDEBAR FILTER ---
    st.sidebar.header("🎛️ Filter Dashboard")
    df_date, df_cat = get_dimensions(engine)
    
    if not df_date.empty:
        available_years = sorted(df_date['year'].unique().tolist(), reverse=True)
        sel_year = st.sidebar.selectbox("Pilih Tahun:", available_years, index=0)
    else:
        st.sidebar.warning("Data tahun tidak ditemukan.")
        st.stop()

    if not df_cat.empty:
        opt_cat = df_cat['category_name'].unique().tolist()
        sel_cat = st.sidebar.multiselect("Filter Kategori:", opt_cat, default=opt_cat)
    else:
        sel_cat = []
        
    if not sel_cat:
        st.warning("Mohon pilih minimal satu kategori produk.")
        st.stop()
        
    category_sql = create_category_filter(sel_cat)

    # === TARGET SETTINGS (BARU!) ===
    st.sidebar.markdown("---")
    st.sidebar.header("🎯 Target Settings")
    
    with st.sidebar.expander("Revenue Target", expanded=False):
        monthly_revenue_target = st.number_input(
            "Target Revenue Bulanan ($)",
            min_value=0,
            value=50000,
            step=5000,
            help="Target pendapatan per bulan"
        )
    
    with st.sidebar.expander("Retention Target", expanded=False):
        retention_target = st.slider(
            "Target Retention Rate (%)",
            min_value=0,
            max_value=100,
            value=70,
            step=5,
            help="Target persentase customer retention"
        )

    # --- FETCH DATA ---
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
        df_retention_prev = get_kpi_data(engine, 'retention_rate', sel_year - 1, category_sql)

    # --- MAIN CONTENT ---
    st.title("🚀 Northwind Strategic Dashboard")
    st.caption(f"Tahun Analisis: {sel_year}")

    # --- KPI SCORECARDS ---
    col1, col2, col3, col4 = st.columns(4)
    
    total_revenue = df_trend['total_revenue'].sum() if not df_trend.empty else 0
    avg_retention = df_retention['retention_rate'].mean() if not df_retention.empty else 0
    active_customers = len(df_clv) if not df_clv.empty else 0
    top_product = df_prod.iloc[0]['product_name'] if not df_prod.empty else "-"
    avg_retention_prev = df_retention_prev['retention_rate'].mean() if not df_retention_prev.empty else 0
    retention_delta = avg_retention - avg_retention_prev
    
    # Hitung Achievement
    total_target = monthly_revenue_target * 12
    revenue_achievement = calculate_achievement(total_revenue, total_target)
    
    col1.metric(
        "💰 Total Revenue", 
        f"${total_revenue:,.0f}",
        delta=f"{revenue_achievement}% vs Target"
    )
    col2.metric(
        "🔄 Avg Retention", 
        f"{avg_retention:.1f}%", 
        delta=f"{retention_delta:+.1f}% vs Last Year"
    )
    col3.metric("👥 Active Customers", f"{active_customers}", delta="Base")
    col4.metric("🏆 Top Product", top_product[:15]+"..." if len(top_product)>15 else top_product, delta="Leader")

    # --- INSIGHTS ---
    st.markdown("---")
    ins = generate_smart_insights(df_trend, df_retention, df_prod)
    
    with st.expander("💡 SUMMARY & AUTOMATED INSIGHTS", expanded=True):
        c1, c2, c3 = st.columns(3)
        
        with c1:
            if ins['finance']:
                st.info(f"**{ins['finance']['title']}**\n\n### {ins['finance']['value']}\n{ins['finance']['detail']}")
        
        with c2:
            if ins['customer']:
                content = f"**{ins['customer']['title']}**\n\n### {ins['customer']['value']}\n{ins['customer']['detail']}"
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
                st.success(f"**{ins['product']['title']}**\n\n### {prod_name}\n{ins['product']['detail']}", icon="🏆")

    # --- TABS ---
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Strategi Keuangan", "🌍 Geografi Pasar", "🤝 Loyalitas Pelanggan", "📦 Efisiensi Produk"])

    # === TAB 1: FINANCIAL (UPDATED WITH TARGET) ===
    with tab1:
        st.subheader(f"Analisis Keuangan & Target Pencapaian - {sel_year}")
        
        # Chart Comparison
        if not df_trend.empty:
            fig_rev_comp = create_revenue_comparison_chart(df_trend, monthly_revenue_target)
            st.plotly_chart(fig_rev_comp, use_container_width=True)
            
            col_sum1, col_sum2 = st.columns(2)
            
            with col_sum1:
                if not df_cat_perf.empty:
                    fig_pie = px.pie(
                        df_cat_perf,
                        names='category_name',
                        values='total_revenue',
                        title='Proporsi Revenue per Kategori',
                        hole=0.4
                    )
                    fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_pie, use_container_width=True)
            
            with col_sum2:
                    st.markdown("##### 📊 Monthly Performance Summary")
                    df_trend['achievement_pct'] = df_trend.apply(lambda row: calculate_achievement(row['total_revenue'], monthly_revenue_target), axis=1)
                    display_df = df_trend[['month_name', 'total_revenue', 'target', 'achievement_pct']].copy()
                    display_df.columns = ['Month', 'Actual ($)', 'Target ($)', 'Achievement (%)']
                    
                    st.dataframe(
                        display_df.style.format({
                            'Actual ($)': '${:,.0f}',
                            'Target ($)': '${:,.0f}',
                            'Achievement (%)': '{:.1f}%'
                        }).background_gradient(cmap='RdYlGn', subset=['Achievement (%)']),
                        use_container_width=True,
                        hide_index=True
                    )

    # === TAB 2: GEOGRAPHIC ===
    with tab2:
        st.subheader("Sebaran Penjualan Global")
        st.markdown("Analisis pasar berdasarkan lokasi pelanggan untuk mengidentifikasi wilayah potensial.")
        
        if not df_geo.empty:
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
            
            with st.expander("🌍 Lihat Data Detail Per Negara"):
                st.dataframe(
                    df_geo.style.format({'total_revenue': '${:,.2f}'})
                    .background_gradient(cmap='Blues', subset=['total_revenue']),
                    use_container_width=True
                )

    # === TAB 3: CUSTOMER (UPDATED WITH TARGET & RFM) ===
    with tab3:
        st.subheader("Analisis Customer Retention & Target")
        
        # Retention Comparison Chart
        if not df_retention.empty:
            fig_ret_comp, df_ret_target = create_retention_comparison_chart(df_retention, retention_target)
            st.plotly_chart(fig_ret_comp, use_container_width=True)
            
            # Performance Summary
            months_above_target = len(df_ret_target[df_ret_target['retention_rate'] >= retention_target])
            avg_gap = df_ret_target['gap'].mean()
            
            col_ret1, col_ret2, col_ret3 = st.columns(3)
            col_ret1.metric("Bulan di Atas Target", f"{months_above_target}/12")
            col_ret2.metric("Avg Gap vs Target", f"{avg_gap:+.1f}%")
            col_ret3.metric("Best Month", df_ret_target.loc[df_ret_target['retention_rate'].idxmax(), 'month'])
        
        st.markdown("---")
        st.subheader("Segmentasi Pelanggan (RFM Analysis)")
        with st.expander("ℹ️ Penjelasan Kategori Pelanggan (Klik untuk Membuka)"):
            st.markdown("""
            Pelanggan dikelompokkan berdasarkan **Recency** (hari sejak belanja terakhir) dan **Frequency** (jumlah transaksi):
            
            1.  🥇 **Champions:** Pelanggan terbaik! Baru saja berbelanja (Recency kecil) dan sangat sering bertransaksi.
            2.  💎 **Loyal Customers:** Pelanggan setia yang berbelanja secara konsisten.
            3.  🌱 **Potential / New:** Pelanggan baru atau pelanggan yang berpotensi menjadi loyal jika dirawat.
            4.  ⚠️ **At Risk:** Pelanggan yang dulunya sering belanja, tapi **sudah lama menghilang**. Perlu segera dihubungi!
            5.  💤 **Lost:** Pelanggan yang sudah lama tidak belanja dan frekuensi belanjanya rendah.
            """)
        
        # CLV & RFM VISUALIZATION (RESTORED SEGMENTATION)
        if not df_rfm_segmented.empty:
            col_rfm1, col_rfm2 = st.columns(2)
            
            rfm_colors = {
                'Champions': '#198754', 'Loyal Customers': '#0dcaf0',
                'Potential': '#ffc107', 'New Customers': '#6f42c1',
                'At Risk': '#fd7e14', 'Lost': '#dc3545'
            }
            
            # 1. Bar Chart: Distribution
            with col_rfm1:
                segment_counts = df_rfm_segmented['Segment'].value_counts().reset_index()
                segment_counts.columns = ['Segment', 'jumlah']
                
                fig_seg = px.bar(
                    segment_counts, x='Segment', y='jumlah',
                    color='Segment', color_discrete_map=rfm_colors,
                    title='Distribusi Pelanggan per Segmen'
                )
                st.plotly_chart(fig_seg, use_container_width=True)

            # 2. Scatter Plot: Recency vs Monetary
            with col_rfm2:
                fig_scatter = px.scatter(
                    df_rfm_segmented, x='recency', y='monetary', color='Segment',
                    size='frequency', hover_name='customer_name',
                    title='Peta Persebaran: Recency vs Monetary',
                    color_discrete_map=rfm_colors
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

            # Data Table
            with st.expander("📋 Lihat Detail Pelanggan per Segmen"):
                st.dataframe(
                    df_rfm_segmented[['customer_name', 'Segment', 'recency', 'frequency', 'monetary']]
                    .sort_values('monetary', ascending=False)
                    .style.format({'monetary': '${:,.0f}'})
                    .background_gradient(cmap='Reds', subset=['recency']),
                    use_container_width=True
                )

    # === TAB 4: PRODUCT ===
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

    # --- DOWNLOAD SECTION (RESTORED PDF) ---
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
            # Pass targets to PDF generator
            pdf_bytes = generate_pdf(data_export, str(sel_year), monthly_revenue_target, retention_target)
            st.sidebar.download_button(
                label="Klik untuk Download PDF",
                data=pdf_bytes,
                file_name=f"Northwind_Report_{sel_year}.pdf",
                mime="application/pdf"
            )

if __name__ == "__main__":
    main()