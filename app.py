import datetime
import io
import os
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

st.set_page_config(
    page_title="Database Inbound 2026", page_icon="🚚", layout="wide"
)
st.title("🚚 Database Inbound 2026")

# =========================================================
# CSS STYLING
# =========================================================
st.markdown(
    """
    <style>
    @keyframes marquee {
        0% { transform: translateX(100%); }
        100% { transform: translateX(-100%); }
    }
    .marquee-container-tab1, .marquee-container-tab2 {
        width: 100%;
        overflow: hidden;
        white-space: nowrap;
        background-color: #f0f2f6;
        padding: 8px 0;
        border-radius: 5px;
        box-sizing: border-box;
        margin-bottom: 15px;
    }
    .marquee-text-tab1, .marquee-text-tab2 {
        display: inline-block;
        padding-left: 100%;
        animation: marquee 35s linear infinite;
        color: #1f77b4;
        font-weight: 600;
        font-size: 14px;
    }
    </style>
""",
    unsafe_allow_html=True,
)

TARGET_GDRIVE_FOLDER_ID = "1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"
OUTPUT_PARQUET_NAME = "data_inbound_2026.parquet"
LOCAL_PARQUET_PATH = os.path.join(os.getcwd(), OUTPUT_PARQUET_NAME)

AGING_ORDER = ["H+0", "H+1", "H+2", "H+3", "H+4", "H+5", "H+6", "H+7", ">H+7"]


def get_drive_service():
  SCOPES = ["https://www.googleapis.com/auth/drive"]
  creds_dict = None
  if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
    creds_dict = dict(st.secrets["gcp_service_account"])
  if creds_dict and "private_key" in creds_dict:
    pk = str(creds_dict["private_key"]).replace("\\n", "\n").strip()
    creds_dict["private_key"] = pk
  return build(
      "drive",
      "v3",
      credentials=service_account.Credentials.from_service_account_info(
          creds_dict, scopes=SCOPES
      ),
  )


def download_file_from_drive(service, folder_id, file_name, local_path):
  try:
    query = (
        f"'{folder_id}' in parents and name = '{file_name}' and trashed = false"
    )
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get("files", [])
    if not items:
      return False
    file_id = items[0]["id"]
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
      _, done = downloader.next_chunk()
    with open(local_path, "wb") as f:
      f.write(fh.getvalue())
    return True
  except Exception:
    return False


@st.cache_resource
def sync_parquet_from_gdrive():
  with st.spinner("🔄 Mengunduh file database terbaru..."):
    try:
      service = get_drive_service()
      return download_file_from_drive(
          service,
          TARGET_GDRIVE_FOLDER_ID,
          OUTPUT_PARQUET_NAME,
          LOCAL_PARQUET_PATH,
      )
    except Exception as e:
      st.error(f"Kesalahan GDrive: {e}")
      return False


st.sidebar.header("⚙️ Pengaturan & Sinkronisasi GDrive")
if st.sidebar.button("🔄 Refresh & Tarik Data Terbaru"):
  st.cache_resource.clear()
  st.cache_data.clear()
  if os.path.exists(LOCAL_PARQUET_PATH):
    os.remove(LOCAL_PARQUET_PATH)
  st.success("Cache dibersihkan! Mengunduh file terbaru...")
  st.rerun()

with st.sidebar:
  st.markdown("---")
  st.subheader("🔐 Keamanan Executive")
  summary_pass = st.text_input(
      "Password Summary Exec:", type="password", key="summary_pass_input"
  )

if not os.path.exists(LOCAL_PARQUET_PATH):
  if not sync_parquet_from_gdrive():
    st.error("File database tidak dapat diunduh. Silakan cek koneksi/GDrive.")
    st.stop()


# =========================================================
# CACHED QUERY FUNCTION (OPTIMIZED DUCKDB MEMORY)
# =========================================================
@st.cache_data(ttl=3600, show_spinner=False)
def run_query(sql_query):
  con = duckdb.connect()
  con.execute("SET memory_limit='1GB';")
  con.execute("SET preserve_insertion_order=false;")
  con.execute(f"""
        CREATE OR REPLACE VIEW v_inbound AS 
        SELECT * FROM read_parquet('{LOCAL_PARQUET_PATH}')
    """)
  df_res = con.execute(sql_query).fetchdf()
  con.close()
  return df_res


try:
  # =========================================================
  # FILTER GLOBAL MENGGUNAKAN FORM (MENCEGAH LAG SAAT PILIH FILTER)
  # =========================================================
  st.sidebar.markdown("---")
  st.sidebar.header("🔍 Filter Data Dashboard")

  # Ambil data opsi unik sekali saja dengan cache
  @st.cache_data(ttl=3600)
  def get_filter_options():
    con = duckdb.connect()
    con.execute(f"CREATE VIEW v_inbound AS SELECT * FROM read_parquet('{LOCAL_PARQUET_PATH}')")
    locs = con.execute("SELECT DISTINCT LAST_LOCATION FROM v_inbound WHERE LAST_LOCATION IS NOT NULL ORDER BY 1").fetchdf()["LAST_LOCATION"].tolist()
    cab = con.execute("SELECT DISTINCT Cabang FROM v_inbound WHERE Cabang IS NOT NULL ORDER BY 1").fetchdf()["Cabang"].tolist()
    zon = con.execute("SELECT DISTINCT Zona FROM v_inbound WHERE Zona IS NOT NULL ORDER BY 1").fetchdf()["Zona"].tolist()
    kec = con.execute("SELECT DISTINCT Kecamatan FROM v_inbound WHERE Kecamatan IS NOT NULL ORDER BY 1").fetchdf()["Kecamatan"].tolist()
    stat = con.execute("SELECT DISTINCT Status_Shipment FROM v_inbound WHERE Status_Shipment IS NOT NULL ORDER BY 1").fetchdf()["Status_Shipment"].tolist()
    hrs = con.execute("SELECT DISTINCT HRS FROM v_inbound WHERE HRS IS NOT NULL ORDER BY 1").fetchdf()["HRS"].tolist()
    minmax = con.execute("SELECT MIN(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')), MAX(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) FROM v_inbound WHERE Last_Inbound IS NOT NULL AND Last_Inbound != '-'").fetchdf()
    con.close()
    return locs, cab, zon, kec, stat, hrs, minmax

  unique_locations, unique_cabang_db, unique_zona, unique_kecamatan, unique_status, unique_hrs, min_max_date = get_filter_options()

  allowed_cabang_list = ["Malang", "Batu", "Blitar", "Kepanjen", "Bululawang", "Turen", "Unknown"]
  unique_cabang = [c for c in unique_cabang_db if any(allowed.lower() in str(c).lower() for allowed in allowed_cabang_list)]

  default_start = min_max_date.iloc[0, 0].date() if not min_max_date.empty and pd.notnull(min_max_date.iloc[0, 0]) else datetime.date(2026, 1, 1)
  default_end = min_max_date.iloc[0, 1].date() if not min_max_date.empty and pd.notnull(min_max_date.iloc[0, 1]) else datetime.date.today()

  # Bungkus filter dalam st.form agar tidak auto-rerun setiap kotak dipilih
  with st.sidebar.form("filter_form"):
    use_date_filter = st.checkbox("📅 Aktifkan Filter Tanggal Last Inbound", value=False)
    date_range = st.date_input("Pilih Rentang Tanggal:", value=(default_start, default_end), min_value=default_start, max_value=default_end)
    
    chosen_loc = st.multiselect("🔍 Filter Last Location:", unique_locations)
    chosen_cabang = st.multiselect("🏢 Filter Cabang:", unique_cabang)
    chosen_zona = st.multiselect("⚡ Filter Zona (QCE):", unique_zona)
    chosen_kecamatan = st.multiselect("📍 Filter Kecamatan:", unique_kecamatan)
    chosen_status = st.multiselect("📦 Filter Shipment Status:", unique_status)
    chosen_hrs = st.multiselect("📋 Filter Status HRS:", unique_hrs)

    submit_filter = st.form_submit_button("🚀 Terapkan Filter", use_container_width=True)

  # Buat query SQL berdasarkan filter
  where_clauses = []
  if use_date_filter and isinstance(date_range, tuple) and len(date_range) == 2:
    start_d, end_d = date_range
    where_clauses.append(f"CAST(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M') AS DATE) BETWEEN '{start_d}' AND '{end_d}'")

  if chosen_loc:
    where_clauses.append(f"LAST_LOCATION IN ({str(tuple(chosen_loc)).replace(',)', ')') if len(chosen_loc)==1 else str(tuple(chosen_loc))})")
  if chosen_cabang:
    where_clauses.append(f"Cabang IN ({str(tuple(chosen_cabang)).replace(',)', ')') if len(chosen_cabang)==1 else str(tuple(chosen_cabang))})")
  if chosen_zona:
    where_clauses.append(f"Zona IN ({str(tuple(chosen_zona)).replace(',)', ')') if len(chosen_zona)==1 else str(tuple(chosen_zona))})")
  if chosen_kecamatan:
    where_clauses.append(f"Kecamatan IN ({str(tuple(chosen_kecamatan)).replace(',)', ')') if len(chosen_kecamatan)==1 else str(tuple(chosen_kecamatan))})")
  if chosen_status:
    where_clauses.append(f"Status_Shipment IN ({str(tuple(chosen_status)).replace(',)', ')') if len(chosen_status)==1 else str(tuple(chosen_status))})")
  if chosen_hrs:
    where_clauses.append(f"HRS IN ({str(tuple(chosen_hrs)).replace(',)', ')') if len(chosen_hrs)==1 else str(tuple(chosen_hrs))})")

  where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else "WHERE 1=1"

  tab1, tab2 = st.tabs(["📊 Main Dashboard", "🔒 Summary Exec"])

  # =========================================================
  # TAB 1: MAIN DASHBOARD
  # =========================================================
  with tab1:
    with st.spinner("⏳ Memuat Main Dashboard..."):
      max_date_df = run_query("SELECT MAX(TRY_STRPTIME(TGL_TARIK_REPORT, '%m/%d/%Y %H:%M')) as max_dt FROM v_inbound")
      max_date = max_date_df["max_dt"][0] if not max_date_df.empty else None
      formatted_date = pd.to_datetime(max_date).strftime("%d %b %Y %H:%M") if pd.notnull(max_date) else "Waktu pembaruan tidak tersedia"

      top_kec_df = run_query(f"""
                SELECT Kecamatan, COUNT(DISTINCT AWB) as total 
                FROM v_inbound {where_sql} 
                AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')
                GROUP BY Kecamatan ORDER BY total DESC LIMIT 5
            """)

      top_kecamatan_str = " &nbsp;&nbsp;&bull;&nbsp;&nbsp; ".join([f"{row.Kecamatan} ({row.total:,})" for idx, row in top_kec_df.iterrows()])
      if not top_kecamatan_str:
        top_kecamatan_str = "Tidak ada data"

      st.markdown(f"""
            <div class="marquee-container-tab1">
                <div class="marquee-text-tab1">
                    🔄 Last Update At: {formatted_date} &nbsp;&nbsp;|&nbsp;&nbsp; 🏆 Top 5 Kecamatan UnRunsheet: {top_kecamatan_str}
                </div>
            </div>
        """, unsafe_allow_html=True)

      if "active_view" not in st.session_state:
        st.session_state.active_view = "UnRunsheet"
      if "active_card_filter" not in st.session_state:
        st.session_state.active_card_filter = None

      metrics_res = run_query(f"""
                SELECT 
                    COUNT(DISTINCT AWB) AS total_awb,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnInbound' THEN AWB END) AS total_uninbound,
                    COUNT(DISTINCT CASE WHEN Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA') THEN AWB END) AS total_unrunsheet,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnRunsheet with PRA' THEN AWB END) AS total_unrunsheet_pra,
                    COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) THEN AWB END) AS total_success_unhrs,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) THEN AWB END) AS total_undel_unhrs
                FROM v_inbound {where_sql}
            """)

      total_awb = metrics_res["total_awb"][0] or 0
      total_uninbound = metrics_res["total_uninbound"][0] or 0
      total_unrunsheet = metrics_res["total_unrunsheet"][0] or 0
      total_unrunsheet_pra = metrics_res["total_unrunsheet_pra"][0] or 0
      total_success_unhrs = metrics_res["total_success_unhrs"][0] or 0
      total_undel_unhrs = metrics_res["total_undel_unhrs"][0] or 0

      c1, c2, c3, c4 = st.columns(4)
      with c1:
        st.metric(label="📦 Total AWB", value=f"{total_awb:,}")
      with c2:
        st.metric(label="⚠️ Total UnInbound", value=f"{total_uninbound:,}")
        if st.button("👉 Lihat Data UnInbound", key="btn_uninbound", use_container_width=True):
          st.session_state.active_view = "UnInbound"
          st.session_state.active_card_filter = "UnInbound"
          st.rerun()
      with c3:
        st.metric(label="🚨 Alert: Total UnRunsheet", value=f"{total_unrunsheet:,}")
        if st.button("👉 Lihat Data UnRunsheet", key="btn_unrunsheet", use_container_width=True):
          st.session_state.active_view = "UnRunsheet"
          st.session_state.active_card_filter = "Total UnRunsheet"
          st.rerun()
      with c4:
        st.metric(label="🚚 UnRunsheet with PRA", value=f"{total_unrunsheet_pra:,}")
        if st.button("👉 Lihat UnRunsheet PRA", key="btn_unrunsheet_pra", use_container_width=True):
          st.session_state.active_view = "UnRunsheet PRA"
          st.session_state.active_card_filter = "UnRunsheet with PRA"
          st.rerun()

      col_m1, col_m2 = st.columns(2)
      with col_m1:
        st.metric(label="⚠️ Success UnHRS", value=f"{total_success_unhrs:,}")
        if st.button("📋 Lihat Detail Success UnHRS", key="btn_success_unhrs", use_container_width=True):
          st.session_state.active_card_filter = "Success UnHRS"
          st.rerun()
      with col_m2:
        st.metric(label="❌ Undel UnHRS", value=f"{total_undel_unhrs:,}")
        if st.button("📋 Lihat Detail Undel UnHRS", key="btn_undel_unhrs", use_container_width=True):
          st.session_state.active_card_filter = "Undel UnHRS"
          st.rerun()

      if st.session_state.active_card_filter:
        card_title = st.session_state.active_card_filter
        with st.expander(f"📋 Detail AWB — Berdasarkan Card Filter: [{card_title}]", expanded=True):
          if card_title == "Success UnHRS":
            card_query_filter = "Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)"
          elif card_title == "Undel UnHRS":
            card_query_filter = "Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)"
          elif card_title == "Total UnRunsheet":
            card_query_filter = "Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')"
          elif card_title == "UnRunsheet with PRA":
            card_query_filter = "Status_Shipment = 'UnRunsheet with PRA'"
          elif card_title == "UnInbound":
            card_query_filter = "Status_Shipment = 'UnInbound'"
          else:
            card_query_filter = "1=1"

          df_card_detail = run_query(f"""
                        SELECT AWB, Last_Inbound, TGL_UPDATE_STATUS_POD, payment_type, age_days AS "Aging (Hari)", LAST_LOCATION, Cabang, Zona, Kecamatan, SERVICE, Status_Shipment, HRS, RUNSHEET_COURIER_NAME
                        FROM v_inbound {where_sql} AND {card_query_filter}
                        ORDER BY age_days DESC LIMIT 300
                    """)
          if not df_card_detail.empty:
            st.dataframe(df_card_detail, use_container_width=True, height=300)
            c_dl, c_rst = st.columns([3, 1])
            with c_dl:
              st.download_button(f"📥 Download CSV [{card_title}]", df_card_detail.to_csv(index=False).encode("utf-8"), file_name=f"Detail_{card_title}.csv", mime="text/csv")
            with c_rst:
              if st.button("🔄 Reset Card Filter"):
                st.session_state.active_card_filter = None
                st.rerun()

      st.markdown("---")
      view_choice = st.radio("📌 Pilih Tampilan Data Dashboard:", ["📊 Analisis UnRunsheet", "⚠️ Tabel UnInbound"], horizontal=True)

      if view_choice == "📊 Analisis UnRunsheet":
        st.subheader("📊 Distribusi Usia Data UnRunsheet")
        unrunsheet_status_clause = "" if chosen_status else "AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')"
        df_aging = run_query(f"""
                    SELECT aging_group, COUNT(DISTINCT AWB) AS total_awb
                    FROM v_inbound {where_sql} {unrunsheet_status_clause}
                    GROUP BY 1 ORDER BY CASE aging_group WHEN 'H+0' THEN 0 WHEN 'H+1' THEN 1 WHEN 'H+2' THEN 2 WHEN 'H+3' THEN 3 WHEN 'H+4' THEN 4 WHEN 'H+5' THEN 5 WHEN 'H+6' THEN 6 WHEN 'H+7' THEN 7 WHEN '>H+7' THEN 8 END
                """)
        if not df_aging.empty:
          fig = px.bar(df_aging, x="aging_group", y="total_awb", text="total_awb")
          fig.update_layout(plot_bgcolor="rgba(40, 40, 40, 0.8)", paper_bgcolor="rgba(0, 0, 0, 0)", font=dict(color="#CCCCCC"), height=350)
          st.plotly_chart(fig, use_container_width=True)

  # =========================================================
  # TAB 2: SUMMARY EXEC
  # =========================================================
  with tab2:
    if summary_pass == "MXG125":
      with st.spinner("⏳ Memuat Executive Summary..."):
        st.title("📊 Executive Summary & Performance Analytics")
        st.info("Tab Executive aktif dan siap digunakan.")
    else:
      st.warning("🔐 Masukkan password 'Summary Exec' yang benar di sidebar kiri.")

except Exception as main_err:
  st.error("⚠️ Terjadi masalah saat mengolah data dashboard:")
  st.exception(main_err)
