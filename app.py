import io
import os
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# =========================================================
# KONFIGURASI HALAMAN STREAMLIT
# =========================================================
st.set_page_config(
    page_title="Database Inbound 2026", page_icon="🚚", layout="wide"
)

st.title("🚚 Database Inbound 2026")

TARGET_GDRIVE_FOLDER_ID = "1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"
SUPPORTING_GDRIVE_FOLDER_ID = "1EsUiz1Egqzlbk0gpaohr7lZBcR5rqB2Y"

OUTPUT_PARQUET_NAME = "data_inbound_2026.parquet"
SYSCODE_FILE_NAME = "Syscode.xlsx"
USER_ID_FILE_NAME = "User_Id_MXG.xlsx"

LOCAL_PARQUET_PATH = os.path.join(os.getcwd(), OUTPUT_PARQUET_NAME)
LOCAL_SYSCODE_PATH = os.path.join(os.getcwd(), SYSCODE_FILE_NAME)
LOCAL_USER_ID_PATH = os.path.join(os.getcwd(), USER_ID_FILE_NAME)


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
def sync_all_files_from_gdrive():
  with st.spinner("🔄 Mengunduh file database dan data pendukung terbaru..."):
    try:
      service = get_drive_service()
      success_parquet = download_file_from_drive(
          service,
          TARGET_GDRIVE_FOLDER_ID,
          OUTPUT_PARQUET_NAME,
          LOCAL_PARQUET_PATH,
      )
      download_file_from_drive(
          service,
          SUPPORTING_GDRIVE_FOLDER_ID,
          SYSCODE_FILE_NAME,
          LOCAL_SYSCODE_PATH,
      )
      download_file_from_drive(
          service,
          SUPPORTING_GDRIVE_FOLDER_ID,
          USER_ID_FILE_NAME,
          LOCAL_USER_ID_PATH,
      )
      return success_parquet
    except Exception as e:
      st.error(f"Kesalahan GDrive: {e}")
      return False


# =========================================================
# SIDEBAR REFRESH & SINKRONISASI
# =========================================================
st.sidebar.header("⚙️ Pengaturan & Sinkronisasi GDrive")
if st.sidebar.button("🔄 Refresh & Tarik Data Terbaru"):
  st.cache_resource.clear()
  for path in [LOCAL_PARQUET_PATH, LOCAL_SYSCODE_PATH, LOCAL_USER_ID_PATH]:
    if os.path.exists(path):
      os.remove(path)
  st.success("Cache dibersihkan! Mengunduh file terbaru...")
  st.rerun()

if not os.path.exists(LOCAL_PARQUET_PATH):
  if not sync_all_files_from_gdrive():
    st.error("File database tidak dapat diunduh. Silakan cek koneksi/GDrive.")
    st.stop()

# =========================================================
# UTAMA: PROSES DATA SANGAT HEMAT RAM VIA DUCKDB VIEW
# =========================================================
try:
  conn = duckdb.connect()
  conn.execute("PRAGMA memory_limit='512MB';")  # Batasi memory DuckDB

  # Load Syscode
  if os.path.exists(LOCAL_SYSCODE_PATH):
    try:
      df_syscode = pd.read_excel(LOCAL_SYSCODE_PATH, engine="openpyxl")
      conn.register("syscode_tbl", df_syscode)
    except Exception:
      conn.register(
          "syscode_tbl",
          pd.DataFrame(
              columns=[
                  "SYSCODE",
                  "KABUPATEN",
                  "ZONA DELEVERY (QCE)",
                  "KECAMATAN",
              ]
          ),
      )
  else:
    conn.register(
        "syscode_tbl",
        pd.DataFrame(
            columns=["SYSCODE", "KABUPATEN", "ZONA DELEVERY (QCE)", "KECAMATAN"]
        ),
    )

  # Load User ID
  if os.path.exists(LOCAL_USER_ID_PATH):
    try:
      df_user_id = pd.read_excel(LOCAL_USER_ID_PATH, engine="openpyxl")
      conn.register("user_id_tbl", df_user_id)
    except Exception:
      conn.register(
          "user_id_tbl", pd.DataFrame(columns=["USER_ID", "USER_ZONE_NAME"])
      )
  else:
    conn.register(
        "user_id_tbl", pd.DataFrame(columns=["USER_ID", "USER_ZONE_NAME"])
    )

  # Inspeksi kolom Parquet secara dinamis
  parquet_cols = [
      col[0]
      for col in conn.execute(
          f"DESCRIBE SELECT * FROM read_parquet('{LOCAL_PARQUET_PATH}')"
      ).fetchall()
  ]

  wus_outgoing_col = (
      "WUS_OUTGOING_CODE" if "WUS_OUTGOING_CODE" in parquet_cols else "NULL"
  )
  status_pod_col = "STATUS_POD" if "STATUS_POD" in parquet_cols else "NULL"
  pod_cat_col = (
      "POD_Category"
      if "POD_Category" in parquet_cols
      else ("CATEGORY" if "CATEGORY" in parquet_cols else "NULL")
  )
  pod_desc_col = (
      "POD_DESC"
      if "POD_DESC" in parquet_cols
      else ("DESCRIPTION" if "DESCRIPTION" in parquet_cols else "NULL")
  )

  # Buat Temporary View di DuckDB
  conn.execute(f"""
        CREATE OR REPLACE VIEW main_view AS 
        WITH raw AS (
            SELECT * FROM read_parquet('{LOCAL_PARQUET_PATH}')
        ),
        parsed AS (
            SELECT 
                *,
                COALESCE(TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(INBOUND_MANIFEST_DATE AS TIMESTAMP)) AS dt_im,
                COALESCE(TRY_STRPTIME(HVO_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVO_DATE AS TIMESTAMP)) AS dt_hvo,
                COALESCE(TRY_STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVI_DATE AS TIMESTAMP)) AS dt_hvi,
                COALESCE(TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_INBOUND_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mti,
                COALESCE(TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_TRANSIT_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mts,
                COALESCE(TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(TRANSIT_MANIFEST_DATE AS TIMESTAMP)) AS dt_tm,
                COALESCE(TRY_STRPTIME(DATE_RUNSHEET, '%m/%d/%Y %H:%M'), TRY_STRPTIME(DATE_RUNSHEET, '%d-%b-%Y %H:%M'), TRY_STRPTIME(DATE_RUNSHEET, '%Y-%m-%d %H:%M'), TRY_CAST(DATE_RUNSHEET AS TIMESTAMP)) AS dt_run,
                COALESCE(TRY_STRPTIME(TGL_RECEIVED, '%m/%d/%Y %H:%M'), TRY_STRPTIME(TGL_RECEIVED, '%d-%b-%Y %H:%M'), TRY_STRPTIME(TGL_RECEIVED, '%Y-%m-%d %H:%M'), TRY_CAST(TGL_RECEIVED AS TIMESTAMP)) AS dt_tgl_rec,
                
                CAST({wus_outgoing_col} AS VARCHAR) AS col_wus_out,
                CAST({status_pod_col} AS VARCHAR) AS col_status_pod,
                CAST({pod_cat_col} AS VARCHAR) AS col_pod_cat,
                CAST({pod_desc_col} AS VARCHAR) AS col_pod_desc
            FROM raw
        ),
        joined AS (
            SELECT 
                p.*,
                u_im.USER_ZONE_NAME AS IM_LOCATION,
                u_mts.USER_ZONE_NAME AS MTS_LOCATION,
                u_mti.USER_ZONE_NAME AS MTI_LOCATION,
                CASE WHEN UPPER(COALESCE(p.TRANSIT_MANIFEST, '')) LIKE '%MXG%' THEN u_tm.USER_ZONE_NAME ELSE NULL END AS TM_LOCATION
            FROM parsed p
            LEFT JOIN user_id_tbl u_im ON p.USER_IM = u_im.USER_ID
            LEFT JOIN user_id_tbl u_mts ON p.MTS_USER = u_mts.USER_ID
            LEFT JOIN user_id_tbl u_mti ON p.MTI_USER = u_mti.USER_ID
            LEFT JOIN user_id_tbl u_tm ON p.TRANSIT_MANIFEST_USER = u_tm.USER_ID
        ),
        calc AS (
            SELECT 
                j.*,
                GREATEST(
                    CASE WHEN UPPER(COALESCE(INBOUND_MANIFEST, '')) LIKE '%MXG%' THEN dt_im ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(HVO_NO, '')) LIKE '%MXG%' THEN dt_hvo ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(MANIFEST_INBOUND_SUBAGEN_NO, '')) LIKE '%MXG%' THEN dt_mti ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(MANIFEST_TRANSIT_SUBAGEN_NO, '')) LIKE '%MXG%' THEN dt_mts ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(TRANSIT_MANIFEST, '')) LIKE '%MXG%' THEN dt_tm ELSE NULL END
                ) AS valid_inbound_date
            FROM joined j
        ),
        processed AS (
            SELECT 
                c.*,
                CASE 
                    WHEN valid_inbound_date IS NULL THEN NULL
                    WHEN valid_inbound_date = dt_im  THEN IM_LOCATION
                    WHEN valid_inbound_date = dt_hvo THEN TRY_CAST(c.HVO_HUB_NAME AS VARCHAR)
                    WHEN valid_inbound_date = dt_hvi THEN TRY_CAST(c.HVO_HUB_DESTINATION_NAME AS VARCHAR)
                    WHEN valid_inbound_date = dt_mti THEN MTI_LOCATION
                    WHEN valid_inbound_date = dt_mts THEN MTS_LOCATION
                    WHEN valid_inbound_date = dt_tm  THEN TM_LOCATION
                    ELSE NULL
                END AS LAST_LOCATION,
                
                CASE 
                    WHEN AWB_CANCEL IS NOT NULL AND TRIM(CAST(AWB_CANCEL AS VARCHAR)) != '' AND UPPER(TRIM(CAST(AWB_CANCEL AS VARCHAR))) != 'N' THEN 'AWB Cancel'
                    WHEN RECEIVING_DATE IS NULL OR TRIM(CAST(RECEIVING_DATE AS VARCHAR)) = '' OR RECEIVING_DATE = '-' THEN 'UnReceiving'
                    WHEN (OUTBOUND_MANIFEST_DATE IS NULL OR TRIM(CAST(OUTBOUND_MANIFEST_DATE AS VARCHAR)) = '' OR OUTBOUND_MANIFEST_DATE = '-')
                         AND NOT (UPPER(SERVICE) LIKE '%CTC%' AND HVO_DATE IS NULL AND UPPER(COALESCE(HVO_NO, '')) LIKE '%MXG%' AND OUTBOUND_MANIFEST_DATE IS NOT NULL) 
                    THEN 'UnOutbound'
                    WHEN (INBOUND_MANIFEST_DATE IS NULL OR TRIM(CAST(INBOUND_MANIFEST_DATE AS VARCHAR)) = '' OR INBOUND_MANIFEST_DATE = '-')
                         AND NOT (UPPER(SERVICE) LIKE '%CTC%' AND HVI_DATE IS NULL AND UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' AND INBOUND_MANIFEST_DATE IS NOT NULL) 
                    THEN 'UnInbound'
                    WHEN UPPER(COALESCE(MANIFEST_TRANSIT_SUBAGEN_NO, '')) LIKE '%MXG%' 
                         AND (MANIFEST_INBOUND_SUBAGEN_NO IS NULL OR TRIM(CAST(MANIFEST_INBOUND_SUBAGEN_NO AS VARCHAR)) = '' OR MANIFEST_INBOUND_SUBAGEN_NO = '-') 
                    THEN 'UnMTI'
                    WHEN (DATE_RUNSHEET IS NULL OR TRIM(CAST(DATE_RUNSHEET AS VARCHAR)) = '' OR DATE_RUNSHEET = '-')
                         AND UPPER(COALESCE(PRA_RUNSHEET_NO, '')) LIKE '%MXG%' 
                    THEN 'UnRunsheet with PRA'
                    WHEN DATE_RUNSHEET IS NULL OR TRIM(CAST(DATE_RUNSHEET AS VARCHAR)) = '' OR DATE_RUNSHEET = '-' 
                    THEN 'UnRunsheet'
                    WHEN (DATE_RUNSHEET IS NULL OR TRIM(CAST(DATE_RUNSHEET AS VARCHAR)) = '' OR DATE_RUNSHEET = '-')
                         AND UPPER(COALESCE(col_wus_out, '')) LIKE '%HOC%'
                    THEN 'UnRunsheet AU (HOC)'
                    WHEN DATE_RUNSHEET IS NOT NULL AND CAST(dt_run AS DATE) = CURRENT_DATE 
                         AND (CODING IS NULL OR TRIM(CAST(CODING AS VARCHAR)) = '' OR CODING = '-') 
                    THEN 'On Delivery Today'
                    WHEN DATE_RUNSHEET IS NOT NULL 
                         AND (CODING IS NULL OR TRIM(CAST(CODING AS VARCHAR)) = '' OR CODING = '-') 
                    THEN 'Open POD'
                    WHEN UPPER(COALESCE(col_status_pod, '')) IN ('SUCCESS', 'RETURN SHIPPER') 
                         AND UPPER(COALESCE(col_wus_out, '')) NOT LIKE '%HRS%' 
                    THEN 'Success UnHRS'
                    WHEN (CODING IS NOT NULL AND TRIM(CAST(CODING AS VARCHAR)) != '' AND CODING != '-')
                         AND (WUS_REMARKS IS NULL OR TRIM(CAST(WUS_REMARKS AS VARCHAR)) = '' OR WUS_REMARKS = '-')
                         AND UPPER(COALESCE(col_pod_cat, '')) NOT IN (
                             'CLAIM MISSING PROCESS', 'CLAIM DAMAGE PROCESS', 'CLOSE BY SYSTEM (ORIGIN)', 
                             'CLOSE BY SYSTEM (DEST)', 'CUSTOMER REQUEST CANCEL', 'CUSTOMER REQUEST', 
                             'SUCCESS CLAIM', 'BREACH', 'KIRIMAN DISITA OLEH BEA CUKAI', 
                             'KIRIMAN DISITA OLEH KEPOLISIAN', 'KIRIMAN RETURN BREACH', 'MISSING'
                         ) 
                    THEN 'UnHRS'
                    ELSE COALESCE(col_pod_cat, 'UNKNOWN')
                END AS Status_Shipment,

                CASE 
                    WHEN dt_tgl_rec IS NOT NULL THEN DATEDIFF('day', dt_tgl_rec, CURRENT_TIMESTAMP)
                    ELSE DATEDIFF('day', valid_inbound_date, CURRENT_TIMESTAMP)
                END AS age_days
            FROM calc c
        )
        SELECT 
            COALESCE(w.AWB, '-') AS AWB,
            COALESCE(w.SERVICE, '-') AS SERVICE,
            COALESCE(w.WUS_REMARKS, '-') AS WUS_REMARKS,
            w.valid_inbound_date AS Last_Inbound,
            COALESCE(w.LAST_LOCATION, 'UNKNOWN LOCATION') AS LAST_LOCATION,
            COALESCE(s.KABUPATEN, 'UNKNOWN CABANG') AS Cabang,
            COALESCE(s."ZONA DELEVERY (QCE)", 'UNKNOWN ZONA') AS Zona,
            COALESCE(s.KECAMATAN, 'UNKNOWN KECAMATAN') AS Kecamatan,
            w.Status_Shipment AS Status_Shipment,
            w.age_days AS age_days,
            COALESCE(w.col_pod_cat, '-') AS POD_Category,
            COALESCE(w.col_pod_desc, '-') AS POD_DESC,
            CASE 
                WHEN age_days <= 0 THEN 'H+0'
                WHEN age_days = 1 THEN 'H+1'
                WHEN age_days = 2 THEN 'H+2'
                WHEN age_days = 3 THEN 'H+3'
                WHEN age_days = 4 THEN 'H+4'
                WHEN age_days = 5 THEN 'H+5'
                WHEN age_days = 6 THEN 'H+6'
                WHEN age_days = 7 THEN 'H+7'
                ELSE '>H+7'
            END AS aging_group
        FROM processed w
        LEFT JOIN syscode_tbl s ON w.DEST = s.SYSCODE
    """)

  # =========================================================
  # OPSI FILTER SIDEBAR (TARIK RINGAN DARI VIEW)
  # =========================================================
  st.sidebar.markdown("---")
  st.sidebar.header("🔍 Filter Data Dashboard")

  unique_locations = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT LAST_LOCATION FROM main_view WHERE LAST_LOCATION IS"
          " NOT NULL ORDER BY 1"
      ).fetchall()
  ]
  unique_cabang = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT Cabang FROM main_view WHERE Cabang IS NOT NULL ORDER"
          " BY 1"
      ).fetchall()
  ]
  unique_zona = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT Zona FROM main_view WHERE Zona IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]
  unique_kecamatan = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT Kecamatan FROM main_view WHERE Kecamatan IS NOT NULL"
          " ORDER BY 1"
      ).fetchall()
  ]
  unique_status = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT Status_Shipment FROM main_view WHERE Status_Shipment"
          " IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]

  chosen_loc = st.sidebar.selectbox(
      "🔍 Filter Last Location:", ["Semua Last Location"] + unique_locations
  )
  chosen_cabang = st.sidebar.selectbox(
      "🏢 Filter Cabang:", ["Semua Cabang"] + unique_cabang
  )
  chosen_zona = st.sidebar.selectbox(
      "⚡ Filter Zona (QCE):", ["Semua Zona"] + unique_zona
  )
  chosen_kecamatan = st.sidebar.selectbox(
      "📍 Filter Kecamatan:", ["Semua Kecamatan"] + unique_kecamatan
  )
  chosen_status = st.sidebar.selectbox(
      "📦 Filter Shipment Status:", ["Semua Status"] + unique_status
  )

  # SUSUN KONDISI SQL FILTER DARI SIDEBAR
  where_clauses = []
  if chosen_loc != "Semua Last Location":
    where_clauses.append(f"LAST_LOCATION = '{chosen_loc}'")
  if chosen_cabang != "Semua Cabang":
    where_clauses.append(f"Cabang = '{chosen_cabang}'")
  if chosen_zona != "Semua Zona":
    where_clauses.append(f"Zona = '{chosen_zona}'")
  if chosen_kecamatan != "Semua Kecamatan":
    where_clauses.append(f"Kecamatan = '{chosen_kecamatan}'")
  if chosen_status != "Semua Status":
    where_clauses.append(f"Status_Shipment = '{chosen_status}'")

  where_sql = (
      "WHERE " + " AND ".join(where_clauses) if where_clauses else "WHERE 1=1"
  )

  # =========================================================
  # RUNNING TEXT & METRIK KARTU
  # =========================================================
  try:
    max_date_df = conn.execute(f"""
            SELECT MAX(
                COALESCE(
                    TRY_STRPTIME(TGL_TARIK_REPORT, '%m/%d/%Y %H:%M'), 
                    TRY_STRPTIME(TGL_TARIK_REPORT, '%d-%b-%Y %H:%M'), 
                    TRY_STRPTIME(TGL_TARIK_REPORT, '%Y-%m-%d %H:%M'), 
                    TRY_CAST(TGL_TARIK_REPORT AS TIMESTAMP)
                )
            ) as max_dt 
            FROM read_parquet('{LOCAL_PARQUET_PATH}')
        """).fetchdf()
    max_date = max_date_df["max_dt"][0] if not max_date_df.empty else None
    formatted_date = (
        pd.to_datetime(max_date).strftime("%d %b %Y %H:%M")
        if pd.notnull(max_date)
        else "Waktu pembaruan tidak tersedia"
    )
  except Exception:
    formatted_date = "Format tanggal tidak dikenali"

  top_kec_df = conn.execute(f"""
        SELECT Kecamatan, COUNT(DISTINCT AWB) as total 
        FROM main_view {where_sql} 
        GROUP BY Kecamatan ORDER BY total DESC LIMIT 5
    """).fetchdf()

  top_kecamatan_str = " &nbsp;&nbsp;&bull;&nbsp;&nbsp; ".join(
      [f"{row.Kecamatan} ({row.total:,})" for idx, row in top_kec_df.iterrows()]
  )
  if not top_kecamatan_str:
    top_kecamatan_str = "Tidak ada data"

  st.markdown(
      f"""
      <marquee style="color: #1f77b4; background-color: #f0f2f6; padding: 8px; border-radius: 5px; font-weight: 600; font-size: 14px;">
          🔄 Last Update At: {formatted_date} &nbsp;&nbsp;|&nbsp;&nbsp; 🏆 Top 5 Kecamatan UnRunsheet: {top_kecamatan_str}
      </marquee>
  """,
      unsafe_allow_html=True,
  )

  if "active_view" not in st.session_state:
    st.session_state.active_view = "UnRunsheet"

  # HITUNG METRIK KARTU DENGAN QUERY HEMAT MEMORI
  metrics_res = conn.execute(f"""
        SELECT 
            COUNT(DISTINCT AWB) AS total_awb,
            COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnInbound' THEN AWB END) AS total_uninbound,
            COUNT(DISTINCT CASE WHEN Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA', 'UnRunsheet AU (HOC)') THEN AWB END) AS total_unrunsheet,
            COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnRunsheet AU (HOC)' THEN AWB END) AS total_unrunsheet_au
        FROM main_view {where_sql}
    """).fetchdf()

  total_awb = metrics_res["total_awb"][0] or 0
  total_uninbound = metrics_res["total_uninbound"][0] or 0
  total_unrunsheet = metrics_res["total_unrunsheet"][0] or 0
  total_unrunsheet_au = metrics_res["total_unrunsheet_au"][0] or 0

  c1, c2, c3, c4 = st.columns(4)
  with c1:
    st.metric(label="📦 Total AWB", value=f"{total_awb:,}")
  with c2:
    st.metric(label="⚠️ Total UnInbound", value=f"{total_uninbound:,}")
    if st.button(
        "👉 Lihat Data UnInbound", key="btn_uninbound", use_container_width=True
    ):
      st.session_state.active_view = "UnInbound"
      st.rerun()
  with c3:
    st.metric(label="🚨 Alert: Total UnRunsheet", value=f"{total_unrunsheet:,}")
    if st.button(
        "👉 Lihat Data UnRunsheet",
        key="btn_unrunsheet",
        use_container_width=True,
    ):
      st.session_state.active_view = "UnRunsheet"
      st.rerun()
  with c4:
    st.metric(label="🚚 Unrunsheet AU (HOC)", value=f"{total_unrunsheet_au:,}")
    if st.button(
        "👉 Lihat UnRunsheet AU",
        key="btn_unrunsheet_au",
        use_container_width=True,
    ):
      st.session_state.active_view = "UnRunsheet AU"
      st.rerun()

  st.markdown("---")

  view_choice = st.radio(
      "📌 Pilih Tampilan Data Dashboard:",
      ["📊 Analisis UnRunsheet", "⚠️ Tabel UnInbound"],
      horizontal=True,
      index=1 if st.session_state.active_view == "UnInbound" else 0,
  )

  if view_choice == "📊 Analisis UnRunsheet":
    st.subheader(
        "📊 Distribusi Usia Data UnRunsheet (Aging dari Max Inbound /"
        " TGL_RECEIVED ke Hari Ini)"
    )

    df_aging = conn.execute(f"""
            SELECT aging_group, COUNT(DISTINCT AWB) AS total_awb
            FROM main_view 
            {where_sql} AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA', 'UnRunsheet AU (HOC)')
            GROUP BY 1
            ORDER BY 
                CASE aging_group
                    WHEN 'H+0' THEN 0 WHEN 'H+1' THEN 1 WHEN 'H+2' THEN 2 WHEN 'H+3' THEN 3 
                    WHEN 'H+4' THEN 4 WHEN 'H+5' THEN 5 WHEN 'H+6' THEN 6 WHEN 'H+7' THEN 7 WHEN '>H+7' THEN 8
                END
        """).fetchdf()

    selected_aging_group = "Semua Aging"
    col_chart, col_table = st.columns([2, 1])

    with col_chart:
      if not df_aging.empty:
        fig = px.bar(
            df_aging,
            x="aging_group",
            y="total_awb",
            text="total_awb",
            labels={"aging_group": "", "total_awb": ""},
        )
        fig.update_traces(
            textposition="outside",
            marker_color="rgba(30, 110, 220, 0.4)",
            marker_line_color="rgb(60, 150, 255)",
            marker_line_width=2,
            width=0.3,
        )
        fig.update_layout(
            plot_bgcolor="rgba(40, 40, 40, 0.8)",
            paper_bgcolor="rgba(0, 0, 0, 0)",
            font=dict(color="#CCCCCC", size=13),
            xaxis=dict(
                type="category",
                tickangle=0,
                gridcolor="rgba(255, 255, 255, 0.05)",
            ),
            yaxis=dict(gridcolor="rgba(255, 255, 255, 0.08)"),
            margin=dict(l=20, r=20, t=30, b=20),
            height=380,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_table:
      st.markdown("**Klik / Pilih Baris Tabel di Bawah:**")
      if not df_aging.empty:
        event = st.dataframe(
            df_aging,
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row",
        )
        if event.selection.get("rows", []):
          selected_aging_group = df_aging.iloc[event.selection["rows"][0]][
              "aging_group"
          ]
          st.success(f"🎯 Filter Aktif: **{selected_aging_group}**")

    st.markdown("---")

    # TABEL MATRIX STATUS SHIPMENT
    st.subheader(
        "📊 Tabel Matrix Status Shipment (Status Shipment ➔ Aging Shipment)"
    )

    pivot_matrix = conn.execute(f"""
            PIVOT (
                SELECT Status_Shipment, aging_group, AWB 
                FROM main_view {where_sql}
            )
            ON aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6', 'H+7', '>H+7')
            USING COUNT(DISTINCT AWB)
            GROUP BY Status_Shipment
        """).fetchdf()

    if not pivot_matrix.empty:
      pivot_matrix = pivot_matrix.fillna(0)
      numeric_cols = [c for c in pivot_matrix.columns if c != "Status_Shipment"]
      pivot_matrix["Grand Total"] = pivot_matrix[numeric_cols].sum(axis=1)
      st.dataframe(pivot_matrix, use_container_width=True)
    else:
      st.info("Tidak ada data untuk kombinasi filter yang dipilih.")

    st.markdown("---")

    # TABEL PIVOT UNRUNSHEET BY LAST LOCATION
    st.subheader(
        "🗂️ Unrunsheet By Last Location (Pivot Hirarki: Last Location ➔ Cabang"
        " ➔ Zona ➔ Kecamatan)"
    )

    pivot_loc = conn.execute(f"""
            PIVOT (
                SELECT LAST_LOCATION, Cabang, Zona, Kecamatan, aging_group, AWB 
                FROM main_view 
                {where_sql} AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA', 'UnRunsheet AU (HOC)')
            )
            ON aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6', 'H+7', '>H+7')
            USING COUNT(DISTINCT AWB)
            GROUP BY LAST_LOCATION, Cabang, Zona, Kecamatan
        """).fetchdf()

    if not pivot_loc.empty:
      pivot_loc = pivot_loc.fillna(0)
      num_cols = [
          c
          for c in pivot_loc.columns
          if c not in ["LAST_LOCATION", "Cabang", "Zona", "Kecamatan"]
      ]
      pivot_loc["Grand Total"] = pivot_loc[num_cols].sum(axis=1)
      st.dataframe(pivot_loc, use_container_width=True)
    else:
      st.info("Tidak ada data Unrunsheet.")

    # TABEL DETAIL AWB (DIBATASI TOP 1,000 AGAR RINGAN)
    st.subheader(
        f"📋 Detail AWB UnRunsheet — Kategori Aging: [{selected_aging_group}]"
    )

    aging_clause = (
        f"AND aging_group = '{selected_aging_group}'"
        if selected_aging_group != "Semua Aging"
        else ""
    )
    df_detail = conn.execute(f"""
            SELECT AWB, Last_Inbound, age_days AS "Aging (Hari)", LAST_LOCATION, Kecamatan, SERVICE, Status_Shipment, POD_Category, POD_DESC, WUS_REMARKS
            FROM main_view
            {where_sql} AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA', 'UnRunsheet AU (HOC)') {aging_clause}
            LIMIT 1000
        """).fetchdf()

    st.dataframe(df_detail, use_container_width=True)

  else:
    st.subheader(
        "⚠️ Tabel UnInbound (Last Inbound Kosong & Tanpa Runsheet MXG)"
    )
    df_uninbound_show = conn.execute(f"""
            SELECT AWB, Cabang, Zona, Kecamatan, age_days AS "Aging (Hari)", SERVICE, Status_Shipment
            FROM main_view
            {where_sql} AND Status_Shipment = 'UnInbound'
            LIMIT 1000
        """).fetchdf()

    if not df_uninbound_show.empty:
      st.dataframe(df_uninbound_show, use_container_width=True)

      csv_u = df_uninbound_show.to_csv(index=False).encode("utf-8")
      st.download_button(
          label="📥 Download Data CSV UnInbound",
          data=csv_u,
          file_name="Data_UnInbound.csv",
          mime="text/csv",
      )
    else:
      st.info("Tidak ada data UnInbound.")

except Exception as main_err:
  st.error("⚠️ Terjadi masalah saat mengolah data dashboard:")
  st.exception(main_err)
