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

# ID Folder GDrive Utama (Parquet) & Folder Pendukung (Syscode & User ID)
TARGET_GDRIVE_FOLDER_ID = "1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"
SUPPORTING_GDRIVE_FOLDER_ID = "1EsUiz1Egqzlbk0gpaohr7lZBcR5rqB2Y"

OUTPUT_PARQUET_NAME = "data_inbound_2026.parquet"
SYSCODE_FILE_NAME = "Syscode.xlsx"
USER_ID_FILE_NAME = "User_Id_MXG.xlsx"

LOCAL_PARQUET_PATH = os.path.join(os.getcwd(), OUTPUT_PARQUET_NAME)
LOCAL_SYSCODE_PATH = os.path.join(os.getcwd(), SYSCODE_FILE_NAME)
LOCAL_USER_ID_PATH = os.path.join(os.getcwd(), USER_ID_FILE_NAME)


# =========================================================
# FUNGSI AKSES GOOGLE DRIVE API & SINKRONISASI FILE
# =========================================================
def get_drive_service():
  SCOPES = ["https://www.googleapis.com/auth/drive"]
  creds_dict = None

  if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
    creds_dict = dict(st.secrets["gcp_service_account"])

  if creds_dict and "private_key" in creds_dict:
    pk = str(creds_dict["private_key"])
    if "\\n" in pk:
      pk = pk.replace("\\n", "\n")
    pk = pk.replace("\r", "").strip()

    start_idx = pk.find("-----BEGIN PRIVATE KEY-----")
    end_idx = pk.find("-----END PRIVATE KEY-----")
    if start_idx != -1 and end_idx != -1:
      pk = pk[start_idx : end_idx + len("-----END PRIVATE KEY-----")]

    creds_dict["private_key"] = pk

  if creds_dict:
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES
    )
  else:
    raise FileNotFoundError(
        "Konfigurasi 'gcp_service_account' tidak ditemukan di st.secrets!"
    )

  return build("drive", "v3", credentials=creds)


def download_file_from_drive(service, folder_id, file_name, local_path):
  try:
    query = f"'{folder_id}' in parents and name = '{file_name}' and trashed = false"
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
      status, done = downloader.next_chunk()
    with open(local_path, "wb") as f:
      f.write(fh.getvalue())
    return True
  except Exception:
    return False


@st.cache_resource
def sync_all_files_from_gdrive():
  """Mengunduh file Parquet dan file Excel pendukung dari Google Drive."""
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
      if not success_parquet:
        st.error(
            f"File '{OUTPUT_PARQUET_NAME}' tidak ditemukan di Google Drive!"
        )
        return False
      return True
    except Exception as e:
      st.error(f"Terjadi kesalahan saat mengunduh file dari Drive: {e}")
      return False


# =========================================================
# SIDEBAR: REFRESH DATA
# =========================================================
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
  success = sync_all_files_from_gdrive()
  if not success:
    st.stop()

# =========================================================
# INISIALISASI DUCKDB & REGISTRASI DATA PENDUKUNG EXCEL
# =========================================================
conn = duckdb.connect()

try:
  if os.path.exists(LOCAL_SYSCODE_PATH):
    df_syscode = pd.read_excel(LOCAL_SYSCODE_PATH)
    conn.register("syscode_tbl", df_syscode)
  if os.path.exists(LOCAL_USER_ID_PATH):
    df_user_id = pd.read_excel(LOCAL_USER_ID_PATH)
    conn.register("user_id_tbl", df_user_id)
except Exception as e:
  st.warning(f"Perhatian saat membaca file Excel pendukung: {e}")

st.success(
    "⚡ File Parquet dan Data Pendukung dimuat secara instan menggunakan"
    " DuckDB!"
)

try:
  # --- QUERY DUCKDB UTAMA UNTUK METRIK & KPI ---
  metrics_query = f"""
        WITH filtered_raw AS (
            SELECT *
            FROM read_parquet('{LOCAL_PARQUET_PATH}')
            WHERE AWB_CANCEL IS NULL 
               OR TRIM(CAST(AWB_CANCEL AS VARCHAR)) = '' 
               OR AWB_CANCEL = '-'
        ),
        parsed_dates AS (
            SELECT 
                *,
                COALESCE(TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(INBOUND_MANIFEST_DATE AS TIMESTAMP)) AS dt_im,
                COALESCE(TRY_STRPTIME(HVO_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVO_DATE AS TIMESTAMP)) AS dt_hvo,
                COALESCE(TRY_STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVI_DATE AS TIMESTAMP)) AS dt_hvi,
                COALESCE(TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_INBOUND_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mti,
                COALESCE(TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_TRANSIT_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mts,
                COALESCE(TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(TRANSIT_MANIFEST_DATE AS TIMESTAMP)) AS dt_tm
            FROM filtered_raw
        ),
        processed_data AS (
            SELECT 
                AWB,
                RUNSHEET_NO,
                SERVICE,
                INBOUND_MANIFEST,
                HVI_NO,
                CONNOTE_RETURN_RT,
                CONNOTE_RETURN_RF,
                WUS_REMARKS,
                
                -- LOGIKA INBOUND
                CASE 
                    WHEN INBOUND_MANIFEST IS NOT NULL AND TRIM(CAST(INBOUND_MANIFEST AS VARCHAR)) != '' AND UPPER(INBOUND_MANIFEST) LIKE '%MXG%' THEN 'VALID'
                    WHEN UPPER(SERVICE) LIKE '%CTC%' AND HVI_NO IS NOT NULL AND TRIM(CAST(HVI_NO AS VARCHAR)) != '' AND UPPER(HVI_NO) LIKE '%MXG%' THEN 'VALID'
                    ELSE 'UNINBOUND'
                END AS status_inbound_flag,
                
                -- LOGIKA UNRUNSHEET
                CASE 
                    WHEN (CONNOTE_RETURN_RT IS NOT NULL AND TRIM(CAST(CONNOTE_RETURN_RT AS VARCHAR)) != '' AND CONNOTE_RETURN_RT != '-')
                      OR (CONNOTE_RETURN_RF IS NOT NULL AND TRIM(CAST(CONNOTE_RETURN_RF AS VARCHAR)) != '' AND CONNOTE_RETURN_RF != '-')
                    THEN 'RUNSHEETED_OR_RETURN'
                    
                    WHEN (RUNSHEET_NO IS NULL OR TRIM(CAST(RUNSHEET_NO AS VARCHAR)) = '' OR RUNSHEET_NO = '-')
                         AND UPPER(COALESCE(WUS_REMARKS, '')) LIKE '%HOC%'
                    THEN 'UNRUNSHEET_AU'
                    
                    WHEN RUNSHEET_NO IS NULL OR TRIM(CAST(RUNSHEET_NO AS VARCHAR)) = '' OR RUNSHEET_NO = '-'
                    THEN 'UNRUNSHEET'
                    
                    ELSE 'RUNSHEETED_OR_RETURN'
                END AS status_runsheet_flag
            FROM parsed_dates
        )
        SELECT 
            COUNT(DISTINCT AWB) AS total_awb,
            COUNT(CASE WHEN status_inbound_flag = 'UNINBOUND' THEN 1 END) AS total_uninbound,
            COUNT(CASE WHEN status_runsheet_flag IN ('UNRUNSHEET', 'UNRUNSHEET_AU') THEN 1 END) AS total_unrunsheet,
            COUNT(CASE WHEN status_runsheet_flag = 'UNRUNSHEET_AU' THEN 1 END) AS total_unrunsheet_au
        FROM processed_data
    """

  metrics_df = conn.execute(metrics_query).fetchdf()

  total_awb = metrics_df["total_awb"][0] or 0
  total_uninbound = metrics_df["total_uninbound"][0] or 0
  total_unrunsheet = metrics_df["total_unrunsheet"][0] or 0
  total_unrunsheet_au = metrics_df["total_unrunsheet_au"][0] or 0

  # --- KARTU METRIK (KPI) ---
  c1, c2, c3, c4 = st.columns(4)
  c1.metric("📦 Total Unique AWB", f"{total_awb:,}")
  c2.metric("⚠️ Total UnInbound", f"{total_uninbound:,}")
  c3.metric(
      "🚨 Alert: Total UnRunsheet",
      f"{total_unrunsheet:,}",
      delta_color="inverse",
  )
  c4.metric("🚚 Unrunsheet AU (HOC)", f"{total_unrunsheet_au:,}")

  st.markdown("---")

  # --- QUERY DISTRIBUSI USIA UNRUNSHEET (GRAFIK BATANG) ---
  st.subheader(
      "📊 Distribusi Usia Data UnRunsheet (Aging dari Max Inbound Date ke"
      " Hari Ini)"
  )

  aging_query = f"""
        WITH filtered_raw AS (
            SELECT *
            FROM read_parquet('{LOCAL_PARQUET_PATH}')
            WHERE AWB_CANCEL IS NULL 
               OR TRIM(CAST(AWB_CANCEL AS VARCHAR)) = '' 
               OR AWB_CANCEL = '-'
        ),
        parsed_dates AS (
            SELECT 
                *,
                COALESCE(TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(INBOUND_MANIFEST_DATE AS TIMESTAMP)) AS dt_im,
                COALESCE(TRY_STRPTIME(HVO_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVO_DATE AS TIMESTAMP)) AS dt_hvo,
                COALESCE(TRY_STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVI_DATE AS TIMESTAMP)) AS dt_hvi,
                COALESCE(TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_INBOUND_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mti,
                COALESCE(TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_TRANSIT_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mts,
                COALESCE(TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(TRANSIT_MANIFEST_DATE AS TIMESTAMP)) AS dt_tm
            FROM filtered_raw
            WHERE NOT (
                (CONNOTE_RETURN_RT IS NOT NULL AND TRIM(CAST(CONNOTE_RETURN_RT AS VARCHAR)) != '' AND CONNOTE_RETURN_RT != '-')
                OR (CONNOTE_RETURN_RF IS NOT NULL AND TRIM(CAST(CONNOTE_RETURN_RF AS VARCHAR)) != '' AND CONNOTE_RETURN_RF != '-')
            )
        ),
        processed_data AS (
            SELECT 
                AWB,
                RUNSHEET_NO,
                GREATEST(
                    CASE WHEN UPPER(COALESCE(INBOUND_MANIFEST, '')) LIKE '%MXG%' THEN dt_im ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(HVO_NO, '')) LIKE '%MXG%' THEN dt_hvo ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(MANIFEST_INBOUND_SUBAGEN_NO, '')) LIKE '%MXG%' THEN dt_mti ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(MANIFEST_TRANSIT_SUBAGEN_NO, '')) LIKE '%MXG%' THEN dt_mts ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(TRANSIT_MANIFEST, '')) LIKE '%MXG%' THEN dt_tm ELSE NULL END
                ) AS valid_inbound_date
            FROM parsed_dates
        ),
        unrunsheet_aging AS (
            SELECT 
                AWB,
                valid_inbound_date,
                DATEDIFF('day', valid_inbound_date, CURRENT_TIMESTAMP) AS age_days
            FROM processed_data
            WHERE (RUNSHEET_NO IS NULL OR TRIM(CAST(RUNSHEET_NO AS VARCHAR)) = '' OR RUNSHEET_NO = '-')
              AND valid_inbound_date IS NOT NULL
        ),
        grouped_aging AS (
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
            FROM unrunsheet_aging
            GROUP BY 1
        )
        SELECT aging_group, total_awb
        FROM grouped_aging
        ORDER BY 
            CASE aging_group
                WHEN 'H+0' THEN 0
                WHEN 'H+1' THEN 1
                WHEN 'H+2' THEN 2
                WHEN 'H+3' THEN 3
                WHEN 'H+4' THEN 4
                WHEN 'H+5' THEN 5
                WHEN 'H+6' THEN 6
                WHEN 'H+7' THEN 7
                WHEN '>H+7' THEN 8
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
          xaxis=dict(
              type="category", tickangle=0, gridcolor="rgba(255, 255, 255, 0.05)"
          ),
          yaxis=dict(gridcolor="rgba(255, 255, 255, 0.08)"),
          margin=dict(l=20, r=20, t=30, b=20),
          height=380,
      )
      st.plotly_chart(fig, use_container_width=True)
    else:
      st.info("Tidak ada data UnRunsheet.")

  with col_table:
    st.markdown("**Klik / Pilih Baris Tabel di Bawah:**")
    event = st.dataframe(
        df_aging,
        use_container_width=True,
        on_select="rerun",
        selection_mode="single-row",
    )
    selected_rows = event.selection.get("rows", [])
    if selected_rows:
      selected_index = selected_rows[0]
      selected_aging_group = df_aging.iloc[selected_index]["aging_group"]
      st.success(f"🎯 Filter Aktif: **{selected_aging_group}**")
    else:
      st.info("💡 *Klik pada baris tabel di atas untuk memfilter detail AWB.*")

  st.markdown("---")

  # =========================================================
  # 🗂️ TABEL PIVOT BERJENJANG DENGAN FILTER CABANG & ZONA
  # =========================================================
  st.subheader(
      "🗂️ Unrunsheet By Last Location (Pivot Hirarki: Last Location ➔ Cabang ➔"
      " Zona ➔ Kecamatan)"
  )

  pivot_query = f"""
        WITH filtered_raw AS (
            SELECT *
            FROM read_parquet('{LOCAL_PARQUET_PATH}')
            WHERE AWB_CANCEL IS NULL 
               OR TRIM(CAST(AWB_CANCEL AS VARCHAR)) = '' 
               OR AWB_CANCEL = '-'
        ),
        parsed_dates AS (
            SELECT 
                *,
                COALESCE(TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(INBOUND_MANIFEST_DATE AS TIMESTAMP)) AS dt_im,
                COALESCE(TRY_STRPTIME(HVO_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVO_DATE AS TIMESTAMP)) AS dt_hvo,
                COALESCE(TRY_STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVI_DATE AS TIMESTAMP)) AS dt_hvi,
                COALESCE(TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_INBOUND_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mti,
                COALESCE(TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_TRANSIT_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mts,
                COALESCE(TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(TRANSIT_MANIFEST_DATE AS TIMESTAMP)) AS dt_tm
            FROM filtered_raw
            WHERE NOT (
                (CONNOTE_RETURN_RT IS NOT NULL AND TRIM(CAST(CONNOTE_RETURN_RT AS VARCHAR)) != '' AND CONNOTE_RETURN_RT != '-')
                OR (CONNOTE_RETURN_RF IS NOT NULL AND TRIM(CAST(CONNOTE_RETURN_RF AS VARCHAR)) != '' AND CONNOTE_RETURN_RF != '-')
            )
        ),
        joined_locations AS (
            SELECT 
                p.*,
                u_im.USER_ZONE_NAME AS IM_LOCATION,
                u_mts.USER_ZONE_NAME AS MTS_LOCATION,
                u_mti.USER_ZONE_NAME AS MTI_LOCATION,
                CASE 
                    WHEN UPPER(COALESCE(p.TRANSIT_MANIFEST, '')) LIKE '%MXG%' THEN u_tm.USER_ZONE_NAME 
                    ELSE NULL 
                END AS TM_LOCATION
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
            WHERE (RUNSHEET_NO IS NULL OR TRIM(CAST(RUNSHEET_NO AS VARCHAR)) = '' OR RUNSHEET_NO = '-')
        ),
        with_last_location AS (
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
                DATEDIFF('day', valid_inbound_date, CURRENT_TIMESTAMP) AS age_days
            FROM calculated c
            WHERE valid_inbound_date IS NOT NULL
        )
        SELECT 
            COALESCE(w.AWB, '-') AS AWB,
            COALESCE(w.SERVICE, '-') AS SERVICE,
            COALESCE(w.WUS_REMARKS, '-') AS WUS_REMARKS,
            w.valid_inbound_date AS tanggal_max_inbound,
            COALESCE(w.LAST_LOCATION, 'UNKNOWN LOCATION') AS LAST_LOCATION,
            COALESCE(s.KABUPATEN, 'UNKNOWN CABANG') AS Cabang,
            COALESCE(s."ZONA DELEVERY (QCE)", 'UNKNOWN ZONA') AS Zona,
            COALESCE(s.KECAMATAN, 'UNKNOWN KECAMATAN') AS Kecamatan,
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
        FROM with_last_location w
        LEFT JOIN syscode_tbl s ON w.DEST = s.SYSCODE
    """

  df_pivot_raw = conn.execute(pivot_query).fetchdf()

  if not df_pivot_raw.empty:
    # --- WIDGET FILTER INTERAKTIF (Last Location, Cabang, Zona) ---
    unique_locations = sorted(df_pivot_raw["LAST_LOCATION"].dropna().unique())
    unique_cabang = sorted(df_pivot_raw["Cabang"].dropna().unique())
    unique_zona = sorted(df_pivot_raw["Zona"].dropna().unique())

    f_col1, f_col2, f_col3 = st.columns(3)
    with f_col1:
      chosen_loc = st.selectbox(
          "🔍 Filter Last Location:",
          ["Semua Last Location"] + list(unique_locations),
      )
    with f_col2:
      chosen_cabang = st.selectbox(
          "🏢 Filter Cabang:", ["Semua Cabang"] + list(unique_cabang)
      )
    with f_col3:
      chosen_zona = st.selectbox(
          "⚡ Filter Zona (QCE):", ["Semua Zona"] + list(unique_zona)
      )

    # Terapkan filter ke dataframe pivot
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

    expected_cols = [
        "H+0",
        "H+1",
        "H+2",
        "H+3",
        "H+4",
        "H+5",
        "H+6",
        "H+7",
        ">H+7",
    ]

    df_pivot = pd.pivot_table(
        filtered_pivot_df,
        index=["LAST_LOCATION", "Cabang", "Zona", "Kecamatan"],
        columns="aging_group",
        values="AWB",
        aggfunc="count",
        fill_value=0,
    )

    existing_cols = [c for c in expected_cols if c in df_pivot.columns]
    other_cols = [c for c in df_pivot.columns if c not in expected_cols]
    df_pivot = df_pivot[existing_cols + other_cols]
    df_pivot["Grand Total"] = df_pivot.sum(axis=1)

    st.dataframe(df_pivot, use_container_width=True)

    with st.expander(
        "📂 Buka Mode Rinci (Expand / Collapse per Last Location)"
    ):
      for loc in unique_locations:
        with st.expander(f"📍 {loc}"):
          sub_df = filtered_pivot_df[filtered_pivot_df["LAST_LOCATION"] == loc]
          if not sub_df.empty:
            sub_pivot = pd.pivot_table(
                sub_df,
                index=["Cabang", "Zona", "Kecamatan"],
                columns="aging_group",
                values="AWB",
                aggfunc="count",
                fill_value=0,
            )
            sub_cols = [c for c in expected_cols if c in sub_pivot.columns]
            sub_pivot = sub_pivot[sub_cols]
            sub_pivot["Grand Total"] = sub_pivot.sum(axis=1)
            st.dataframe(sub_pivot, use_container_width=True)
          else:
            st.info("Tidak ada data untuk filter ini.")
  else:
    st.info("Tidak ada data untuk tabel pivot UnRunsheet.")

  st.markdown("---")

  # =========================================================
  # 📋 TABEL DETAIL AWB UNRUNSHEET (MENGIKUTI FILTER DI ATAS)
  # =========================================================
  st.subheader(
      f"📋 Detail AWB UnRunsheet — Kategori Aging: [{selected_aging_group}]"
  )

  # Filter dataframe detail agar ikut tersaring berdasarkan pilihan Last Location, Cabang, Zona, dan Aging
  df_detail_filtered = df_pivot_raw.copy()

  if selected_aging_group != "Semua Aging":
    df_detail_filtered = df_detail_filtered[
        df_detail_filtered["aging_group"] == selected_aging_group
    ]
  if chosen_loc != "Semua Last Location":
    df_detail_filtered = df_detail_filtered[
        df_detail_filtered["LAST_LOCATION"] == chosen_loc
    ]
  if chosen_cabang != "Semua Cabang":
    df_detail_filtered = df_detail_filtered[
        df_detail_filtered["Cabang"] == chosen_cabang
    ]
  if chosen_zona != "Semua Zona":
    df_detail_filtered = df_detail_filtered[
        df_detail_filtered["Zona"] == chosen_zona
    ]

  # Format kolom untuk ditampilkan di tabel detail
  df_display_detail = df_detail_filtered[
      ["AWB", "tanggal_max_inbound", "aging_group", "SERVICE", "WUS_REMARKS"]
  ].copy()
  df_display_detail["Aging"] = (
      df_display_detail["aging_group"].astype(str) + " (Grup)"
  )
  df_display_detail["Kategori_Unrunsheet"] = df_display_detail[
      "WUS_REMARKS"
  ].apply(
      lambda x: (
          "🚨 Unrunsheet AU"
          if pd.notnull(x) and "HOC" in str(x).upper()
          else "Unrunsheet Biasa"
      )
  )

  df_final_detail = df_display_detail[
      [
          "AWB",
          "tanggal_max_inbound",
          "Aging",
          "SERVICE",
          "Kategori_Unrunsheet",
          "WUS_REMARKS",
      ]
  ].rename(columns={"tanggal_max_inbound": "Tanggal_Max_Inbound"})

  st.dataframe(df_final_detail, use_container_width=True)

  csv_data = df_final_detail.to_csv(index=False).encode("utf-8")
  st.download_button(
      label=(
          f"📥 Download Data CSV UnRunsheet [{selected_aging_group} |"
          f" Cabang: {chosen_cabang} | Zona: {chosen_zona}]"
      ),
      data=csv_data,
      file_name=f"UnRunsheet_{selected_aging_group.replace(' ', '_')}_Cabang_{chosen_cabang}.csv",
      mime="text/csv",
  )

except Exception as e:
  st.error(f"Terjadi kesalahan saat memproses data & pivot table: {e}")
