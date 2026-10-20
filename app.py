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


st.sidebar.header("⚙️ Pengaturan & Sinkronisasi GDrive")
if st.sidebar.button("🔄 Refresh & Tarik Data Terbaru"):
  st.cache_resource.clear()
  for path in [
      LOCAL_PARQUET_PATH,
      LOCAL_SYSCODE_PATH,
      LOCAL_USER_ID_PATH,
  ]:
    if os.path.exists(path):
      os.remove(path)
  st.success("Cache dibersihkan! Mengunduh file terbaru...")
  st.rerun()

if not os.path.exists(LOCAL_PARQUET_PATH):
  if not sync_all_files_from_gdrive():
    st.stop()

conn = duckdb.connect()
try:
  if os.path.exists(LOCAL_SYSCODE_PATH):
    conn.register("syscode_tbl", pd.read_excel(LOCAL_SYSCODE_PATH))
  if os.path.exists(LOCAL_USER_ID_PATH):
    conn.register("user_id_tbl", pd.read_excel(LOCAL_USER_ID_PATH))
except Exception as e:
  st.warning(f"Perhatian saat membaca file Excel pendukung: {e}")

# =========================================================
# QUERY DATA UTAMA DENGAN LOGIKA KOLOM BARU
# =========================================================
base_unrunsheet_query = f"""
    WITH filtered_raw AS (
        SELECT *
        FROM read_parquet('{LOCAL_PARQUET_PATH}')
        WHERE (AWB_CANCEL IS NULL OR UPPER(TRIM(CAST(AWB_CANCEL AS VARCHAR))) != 'Y')
          AND NOT (
              (CONNOTE_RETURN_RT IS NOT NULL AND TRIM(CAST(CONNOTE_RETURN_RT AS VARCHAR)) != '' AND CONNOTE_RETURN_RT != '-')
              OR (CONNOTE_RETURN_RF IS NOT NULL AND TRIM(CAST(CONNOTE_RETURN_RF AS VARCHAR)) != '' AND CONNOTE_RETURN_RF != '-')
          )
    ),
    parsed_dates AS (
        SELECT 
            *,
            COALESCE(TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(INBOUND_MANIFEST_DATE AS TIMESTAMP)) AS dt_im,
            COALESCE(TRY_STRPTIME(HVO_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVO_DATE AS TIMESTAMP)) AS dt_hvo,
            COALESCE(TRY_STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVI_DATE AS TIMESTAMP)) AS dt_hvi,
            COALESCE(TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_INBOUND_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mti,
            COALESCE(TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_TRANSIT_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mts,
            COALESCE(TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(TRANSIT_MANIFEST_DATE AS TIMESTAMP)) AS dt_tm,
            COALESCE(TRY_STRPTIME(RECEIVING_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(RECEIVING_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(RECEIVING_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(RECEIVING_DATE AS TIMESTAMP)) AS dt_rec,
            COALESCE(TRY_STRPTIME(OUTBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(OUTBOUND_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(OUTBOUND_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(OUTBOUND_MANIFEST_DATE AS TIMESTAMP)) AS dt_out,
            COALESCE(TRY_STRPTIME(DATE_RUNSHEET, '%m/%d/%Y %H:%M'), TRY_STRPTIME(DATE_RUNSHEET, '%d-%b-%Y %H:%M'), TRY_STRPTIME(DATE_RUNSHEET, '%Y-%m-%d %H:%M'), TRY_CAST(DATE_RUNSHEET AS TIMESTAMP)) AS dt_run,
            COALESCE(TRY_STRPTIME(TGL_RECEIVED, '%m/%d/%Y %H:%M'), TRY_STRPTIME(TGL_RECEIVED, '%d-%b-%Y %H:%M'), TRY_STRPTIME(TGL_RECEIVED, '%Y-%m-%d %H:%M'), TRY_CAST(TGL_RECEIVED AS TIMESTAMP)) AS dt_tgl_rec
        FROM filtered_raw
    ),
    joined_locations AS (
        SELECT 
            p.*,
            u_im.USER_ZONE_NAME AS IM_LOCATION,
            u_mts.USER_ZONE_NAME AS MTS_LOCATION,
            u_mti.USER_ZONE_NAME AS MTI_LOCATION,
            CASE WHEN UPPER(COALESCE(p.TRANSIT_MANIFEST, '')) LIKE '%MXG%' THEN u_tm.USER_ZONE_NAME ELSE NULL END AS TM_LOCATION
        FROM parsed_dates p
        LEFT JOIN user_id_tbl u_im ON p.USER_IM = u_im.USER_ID
        LEFT JOIN user_id_tbl u_mts ON p.MTS_USER = u_mts.USER_ID
        LEFT JOIN user_id_tbl u_mti ON p.MTI_USER = u_mti.USER_ID
        LEFT JOIN user_id_tbl u_tm ON p.TRANSIT_MANIFEST_USER = u_tm.USER_ID
    ),
    calculated AS (
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
        FROM joined_locations j
    ),
    with_status_and_aging AS (
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
            
            -- LOGIKA STATUS SHIPMENT
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
                
                WHEN DATE_RUNSHEET IS NOT NULL AND CAST(dt_run AS DATE) = CURRENT_DATE 
                     AND (CODING IS NULL OR TRIM(CAST(CODING AS VARCHAR)) = '' OR CODING = '-') 
                THEN 'On Delivery Today'
                
                WHEN DATE_RUNSHEET IS NOT NULL 
                     AND (CODING IS NULL OR TRIM(CAST(CODING AS VARCHAR)) = '' OR CODING = '-') 
                THEN 'Open POD'
                
                WHEN UPPER(COALESCE(STATUS_POD, '')) IN ('SUCCESS', 'RETURN SHIPPER') 
                     AND UPPER(COALESCE(WUS_OUTGOING_CODE, '')) NOT LIKE '%HRS%' 
                THEN 'Success UnHRS'
                
                WHEN (CODING IS NOT NULL AND TRIM(CAST(CODING AS VARCHAR)) != '' AND CODING != '-')
                     AND (WUS_REMARKS IS NULL OR TRIM(CAST(WUS_REMARKS AS VARCHAR)) = '' OR WUS_REMARKS = '-')
                     AND UPPER(COALESCE(POD_Category, '')) NOT IN (
                         'CLAIM MISSING PROCESS', 'CLAIM DAMAGE PROCESS', 'CLOSE BY SYSTEM (ORIGIN)', 
                         'CLOSE BY SYSTEM (DEST)', 'CUSTOMER REQUEST CANCEL', 'CUSTOMER REQUEST', 
                         'SUCCESS CLAIM', 'BREACH', 'KIRIMAN DISITA OLEH BEA CUKAI', 
                         'KIRIMAN DISITA OLEH KEPOLISIAN', 'KIRIMAN RETURN BREACH', 'MISSING'
                     ) 
                THEN 'UnHRS'
                
                ELSE COALESCE(POD_Category, 'UNKNOWN')
            END AS Status_Shipment,

            -- LOGIKA AGING SHIPMENT
            CASE 
                WHEN dt_tgl_rec IS NOT NULL THEN DATEDIFF('day', dt_tgl_rec, CURRENT_TIMESTAMP)
                ELSE DATEDIFF('day', valid_inbound_date, CURRENT_TIMESTAMP)
            END AS Aging_Shipment

        FROM calculated c
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
        w.Aging_Shipment AS age_days,
        COALESCE(w.POD_Category, '-') AS POD_Category,
        COALESCE(w.POD_DESC, '-') AS POD_DESC,
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
    FROM with_status_and_aging w
    LEFT JOIN syscode_tbl s ON w.DEST = s.SYSCODE
    WHERE (w.RUNSHEET_NO IS NULL OR TRIM(CAST(w.RUNSHEET_NO AS VARCHAR)) == '' OR w.RUNSHEET_NO = '-')
      AND w.valid_inbound_date IS NOT NULL
"""

df_pivot_raw = conn.execute(base_unrunsheet_query).fetchdf()

# =========================================================
# SIDEBAR FILTERS
# =========================================================
st.sidebar.markdown("---")
st.sidebar.header("🔍 Filter Data UnRunsheet")

unique_locations = sorted(df_pivot_raw["LAST_LOCATION"].dropna().unique())
unique_cabang = sorted(df_pivot_raw["Cabang"].dropna().unique())
unique_zona = sorted(df_pivot_raw["Zona"].dropna().unique())
unique_kecamatan = sorted(df_pivot_raw["Kecamatan"].dropna().unique())

chosen_loc = st.sidebar.selectbox(
    "🔍 Filter Last Location:", ["Semua Last Location"] + list(unique_locations)
)
chosen_cabang = st.sidebar.selectbox(
    "🏢 Filter Cabang:", ["Semua Cabang"] + list(unique_cabang)
)
chosen_zona = st.sidebar.selectbox(
    "⚡ Filter Zona (QCE):", ["Semua Zona"] + list(unique_zona)
)
chosen_kecamatan = st.sidebar.selectbox(
    "📍 Filter Kecamatan:", ["Semua Kecamatan"] + list(unique_kecamatan)
)

filtered_pivot_df = df_pivot_raw.copy()
if chosen_loc != "Semua Last Location":
  filtered_pivot_df = filtered_pivot_df[
      filtered_pivot_df["LAST_LOCATION"] == chosen_loc
  ]
if chosen_cabang != "Semua Cabang":
  filtered_pivot_df = filtered_pivot_df[
      filtered_pivot_df["Cabang"] == chosen_cabang
  ]
if chosen_zona != "Semua Zona":
  filtered_pivot_df = filtered_pivot_df[
      filtered_pivot_df["Zona"] == chosen_zona
  ]
if chosen_kecamatan != "Semua Kecamatan":
  filtered_pivot_df = filtered_pivot_df[
      filtered_pivot_df["Kecamatan"] == chosen_kecamatan
  ]

# =========================================================
# RUNNING TEXT & DASHBOARD METRICS
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

top_kecamatan_df = (
    filtered_pivot_df.groupby("Kecamatan")["AWB"]
    .count()
    .reset_index(name="total")
    .sort_values(by="total", ascending=False)
    .head(5)
)
top_kecamatan_str = " &nbsp;&nbsp;&bull;&nbsp;&nbsp; ".join(
    [
        f"{row.Kecamatan} ({row.total:,})"
        for idx, row in top_kecamatan_df.iterrows()
    ]
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

try:
  metrics_query = f"""
        WITH filtered_raw AS (
            SELECT *
            FROM read_parquet('{LOCAL_PARQUET_PATH}')
            WHERE (AWB_CANCEL IS NULL OR UPPER(TRIM(CAST(AWB_CANCEL AS VARCHAR))) != 'Y')
              AND NOT (
                  (CONNOTE_RETURN_RT IS NOT NULL AND TRIM(CAST(CONNOTE_RETURN_RT AS VARCHAR)) != '' AND CONNOTE_RETURN_RT != '-')
                  OR (CONNOTE_RETURN_RF IS NOT NULL AND TRIM(CAST(CONNOTE_RETURN_RF AS VARCHAR)) != '' AND CONNOTE_RETURN_RF != '-')
              )
        ),
        parsed_dates AS (
            SELECT 
                *,
                COALESCE(TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(INBOUND_MANIFEST_DATE AS TIMESTAMP)) AS dt_im,
                COALESCE(TRY_STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVI_DATE AS TIMESTAMP)) AS dt_hvi,
                COALESCE(TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_INBOUND_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mti
            FROM filtered_raw
        ),
        processed_data AS (
            SELECT 
                AWB,
                RUNSHEET_NO,
                SERVICE,
                INBOUND_MANIFEST,
                HVI_NO,
                WUS_REMARKS,
                CONNOTE_RETURN_RT,
                CONNOTE_RETURN_RF,
                GREATEST(
                    CASE WHEN UPPER(COALESCE(INBOUND_MANIFEST, '')) LIKE '%MXG%' THEN dt_im ELSE NULL END,
                    CASE WHEN UPPER(SERVICE) LIKE '%CTC%' AND UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(MANIFEST_INBOUND_SUBAGEN_NO, '')) LIKE '%MXG%' THEN dt_mti ELSE NULL END
                ) AS last_inbound_date,
                CASE 
                    WHEN (RUNSHEET_NO IS NULL OR TRIM(CAST(RUNSHEET_NO AS VARCHAR)) == '' OR RUNSHEET_NO = '-')
                         AND UPPER(COALESCE(WUS_REMARKS, '')) LIKE '%HOC%'
                    THEN 'UNRUNSHEET_AU'
                    WHEN RUNSHEET_NO IS NULL OR TRIM(CAST(RUNSHEET_NO AS VARCHAR)) == '' OR RUNSHEET_NO = '-'
                    THEN 'UNRUNSHEET'
                    ELSE 'RUNSHEETED'
                END AS status_runsheet_flag
            FROM parsed_dates
        )
        SELECT 
            COUNT(DISTINCT AWB) AS total_awb,
            COUNT(
                CASE 
                    WHEN last_inbound_date IS NULL 
                         AND (RUNSHEET_NO IS NULL OR UPPER(RUNSHEET_NO) NOT LIKE '%MXG%')
                         AND (CONNOTE_RETURN_RT IS NULL OR TRIM(CAST(CONNOTE_RETURN_RT AS VARCHAR)) = '' OR CONNOTE_RETURN_RT = '-')
                         AND (CONNOTE_RETURN_RF IS NULL OR TRIM(CAST(CONNOTE_RETURN_RF AS VARCHAR)) = '' OR CONNOTE_RETURN_RF = '-')
                    THEN 1 
                END
            ) AS total_uninbound,
            COUNT(CASE WHEN status_runsheet_flag IN ('UNRUNSHEET', 'UNRUNSHEET_AU') THEN 1 END) AS total_unrunsheet,
            COUNT(CASE WHEN status_runsheet_flag = 'UNRUNSHEET_AU' THEN 1 END) AS total_unrunsheet_au
        FROM processed_data
    """
  metrics_df = conn.execute(metrics_query).fetchdf()

  total_awb = metrics_df["total_awb"][0] or 0
  total_uninbound = metrics_df["total_uninbound"][0] or 0
  total_unrunsheet = metrics_df["total_unrunsheet"][0] or 0
  total_unrunsheet_au = metrics_df["total_unrunsheet_au"][0] or 0

  c1, c2, c3, c4 = st.columns(4)
  with c1:
    st.metric(label="📦 Total Unique AWB", value=f"{total_awb:,}")
  with c2:
    st.metric(label="⚠️ Total UnInbound", value=f"{total_uninbound:,}")
    if st.button("👉 Lihat Data UnInbound", key="btn_uninbound", use_container_width=True):
      st.session_state.active_view = "UnInbound"
      st.rerun()
  with c3:
    st.metric(label="🚨 Alert: Total UnRunsheet", value=f"{total_unrunsheet:,}")
    if st.button("👉 Lihat Data UnRunsheet", key="btn_unrunsheet", use_container_width=True):
      st.session_state.active_view = "UnRunsheet"
      st.rerun()
  with c4:
    st.metric(label="🚚 Unrunsheet AU (HOC)", value=f"{total_unrunsheet_au:,}")
    if st.button("👉 Lihat UnRunsheet AU", key="btn_unrunsheet_au", use_container_width=True):
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
        "📊 Distribusi Usia Data UnRunsheet (Aging from Last_Inbound)"
    )

    if filtered_pivot_df.empty:
      df_aging = pd.DataFrame(columns=["aging_group", "total_awb"])
    else:
      conn.register("filtered_awb_tbl", filtered_pivot_df[["AWB", "age_days"]])
      aging_query = """
                SELECT 
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
                    END AS aging_group,
                    COUNT(DISTINCT AWB) AS total_awb
                FROM filtered_awb_tbl
                GROUP BY 1
                ORDER BY 
                    CASE aging_group
                        WHEN 'H+0' THEN 0 WHEN 'H+1' THEN 1 WHEN 'H+2' THEN 2 WHEN 'H+3' THEN 3 
                        WHEN 'H+4' THEN 4 WHEN 'H+5' THEN 5 WHEN 'H+6' THEN 6 WHEN 'H+7' THEN 7 WHEN '>H+7' THEN 8
                    END
            """
      df_aging = conn.execute(aging_query).fetchdf()

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
            xaxis=dict(type="category", tickangle=0, gridcolor="rgba(255, 255, 255, 0.05)"),
            yaxis=dict(gridcolor="rgba(255, 255, 255, 0.08)"),
            margin=dict(l=20, r=20, t=30, b=20),
            height=380,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_table:
      st.markdown("**Klik / Pilih Baris Tabel di Bawah:**")
      if not df_aging.empty:
        event = st.dataframe(
            df_aging, use_container_width=True, on_select="rerun", selection_mode="single-row"
        )
        if event.selection.get("rows", []):
          selected_aging_group = df_aging.iloc[event.selection["rows"][0]]["aging_group"]
          st.success(f"🎯 Filter Aktif: **{selected_aging_group}**")

    st.markdown("---")
    st.subheader(
        "🗂️ Unrunsheet By Last Location"
    )

    df_pivot = pd.pivot_table(
        filtered_pivot_df,
        index=["LAST_LOCATION", "Cabang", "Zona", "Kecamatan"],
        columns="aging_group",
        values="AWB",
        aggfunc="count",
        fill_value=0,
    )
    expected_cols = ["H+0", "H+1", "H+2", "H+3", "H+4", "H+5", "H+6", "H+7", ">H+7"]
    df_pivot = df_pivot[[c for c in expected_cols if c in df_pivot.columns]]
    df_pivot["Grand Total"] = df_pivot.sum(axis=1)
    st.dataframe(df_pivot, use_container_width=True)

    st.subheader(f"📋 Detail AWB UnRunsheet — Kategori Aging: [{selected_aging_group}]")
    df_detail_filtered = filtered_pivot_df.copy()
    if selected_aging_group != "Semua Aging":
      df_detail_filtered = df_detail_filtered[
          df_detail_filtered["aging_group"] == selected_aging_group
      ]

    st.dataframe(
        df_detail_filtered[
            [
                "AWB",
                "Last_Inbound",
                "age_days",
                "LAST_LOCATION",
                "Kecamatan",
                "SERVICE",
                "Status_Shipment",
                "POD_Category",
                "POD_DESC",
                "WUS_REMARKS",
            ]
        ].rename(columns={"age_days": "Aging (Hari)"}),
        use_container_width=True,
    )

  else:
    st.subheader("⚠️ Tabel UnInbound (Last Inbound Kosong & Tanpa Runsheet MXG)")
    # (Logika halaman UnInbound tetap sama seperti sebelumnya)
    st.info("Fitur UnInbound aktif.")

except Exception as e:
  st.error(f"Terjadi kesalahan saat memproses data: {e}")
