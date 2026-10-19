import io
import os
import duckdb
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

# ID Folder INBOUND di Google Drive
TARGET_GDRIVE_FOLDER_ID = "1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"
OUTPUT_PARQUET_NAME = "data_inbound_2026.parquet"
SERVICE_ACCOUNT_FILE = "credentials.json"
LOCAL_PARQUET_PATH = os.path.join(os.getcwd(), OUTPUT_PARQUET_NAME)


# =========================================================
# FUNGSI AKSES GOOGLE DRIVE API & SINKRONISASI PARQUET
# =========================================================
def get_drive_service():
  SCOPES = ["https://www.googleapis.com/auth/drive"]
  # Cek apakah berjalan di Streamlit Cloud dengan secrets
  if "gcp_service_account" in st.secrets:
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES
    )
  else:
    # Jika berjalan di lokal (VS Code) menggunakan file fisik
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
  return build("drive", "v3", credentials=creds)

@st.cache_resource
def sync_parquet_from_gdrive():
  """Mencari file data_inbound_2026.parquet di Drive dan mengunduhnya ke lokal."""
  with st.spinner("🔄 Mengunduh file Parquet terbaru dari Google Drive..."):
    try:
      service = get_drive_service()

      # Cari ID file Parquet di folder Google Drive
      query = f"'{TARGET_GDRIVE_FOLDER_ID}' in parents and name = '{OUTPUT_PARQUET_NAME}' and trashed = false"
      results = (
          service.files().list(q=query, fields="files(id, name)").execute()
      )
      items = results.get("files", [])

      if not items:
        st.error(
            f"File '{OUTPUT_PARQUET_NAME}' tidak ditemukan di folder Google"
            " Drive!"
        )
        return False

      file_id = items[0]["id"]

      # Unduh file Parquet secara efisien
      request = service.files().get_media(fileId=file_id)
      fh = io.BytesIO()
      downloader = MediaIoBaseDownload(fh, request)
      done = False
      while not done:
        status, done = downloader.next_chunk()

      with open(LOCAL_PARQUET_PATH, "wb") as f:
        f.write(fh.getvalue())

      return True
    except Exception as e:
      st.error(f"Terjadi kesalahan saat mengunduh Parquet dari Drive: {e}")
      return False


# =========================================================
# SIDEBAR: REFRESH DATA
# =========================================================
st.sidebar.header("⚙️ Pengaturan & Sinkronisasi GDrive")
if st.sidebar.button("🔄 Refresh & Tarik Data Terbaru"):
  st.cache_resource.clear()
  if os.path.exists(LOCAL_PARQUET_PATH):
    os.remove(LOCAL_PARQUET_PATH)
  st.success("Cache dibersihkan! Mengunduh file Parquet terbaru...")
  st.rerun()

# Unduh Parquet jika belum ada di lokal
if not os.path.exists(LOCAL_PARQUET_PATH):
  success = sync_parquet_from_gdrive()
  if not success:
    st.stop()

# =========================================================
# KHUSUS DUCKDB & QUERY ANALISIS PARQUET
# =========================================================
conn = duckdb.connect()

st.success("⚡ File Parquet dimuat secara instan menggunakan DuckDB!")

try:
  # --- QUERY DUCKDB UNTUK METRIK UTAMA ---
  metrics_query = f"""
        WITH processed_data AS (
            SELECT 
                AWB,
                RUNSHEET_NO,
                SERVICE,
                INBOUND_MANIFEST,
                HVI_NO,
                CASE 
                    WHEN INBOUND_MANIFEST IS NULL OR TRIM(CAST(INBOUND_MANIFEST AS VARCHAR)) = '' OR UPPER(INBOUND_MANIFEST) NOT LIKE 'MXG%' THEN NULL
                    ELSE 
                        GREATEST(
                            CASE WHEN UPPER(INBOUND_MANIFEST) LIKE 'MXG%' AND INBOUND_MANIFEST_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END,
                            CASE WHEN UPPER(HVI_NO) LIKE 'MXG%' AND HVI_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END,
                            CASE WHEN UPPER(MANIFEST_INBOUND_SUBAGEN_NO) LIKE 'MXG%' AND MANIFEST_INBOUND_SUBAGEN_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END
                        )
                END AS valid_inbound_date
            FROM read_parquet('{LOCAL_PARQUET_PATH}')
        )
        SELECT 
            COUNT(DISTINCT AWB) AS total_awb,
            COUNT(CASE WHEN valid_inbound_date IS NULL THEN 1 END) AS total_uninbound,
            COUNT(CASE WHEN RUNSHEET_NO IS NULL OR TRIM(CAST(RUNSHEET_NO AS VARCHAR)) = '' OR RUNSHEET_NO = '-' THEN 1 END) AS total_unrunsheet
        FROM processed_data
    """

  metrics_df = conn.execute(metrics_query).fetchdf()

  total_awb = metrics_df["total_awb"][0] or 0
  total_uninbound = metrics_df["total_uninbound"][0] or 0
  total_unrunsheet = metrics_df["total_unrunsheet"][0] or 0

  # --- TAMPILAN KARTU METRIK (KPI) ---
  c1, c2, c3 = st.columns(3)
  c1.metric("📦 Total Unique AWB (Gabungan)", f"{total_awb:,}")
  c2.metric("⚠️ Total UnInbound", f"{total_uninbound:,}")
  c3.metric(
      "🚨 Alert: Total UnRunsheet",
      f"{total_unrunsheet:,}",
      delta_color="inverse",
  )

  st.markdown("---")

  # --- QUERY DISTRIBUSI USIA UNRUNSHEET ---
  st.subheader(
      "📊 Distribusi Usia Data UnRunsheet (Aging dari Max Inbound Date ke"
      " Hari Ini)"
  )

  aging_query = f"""
        WITH processed_data AS (
            SELECT 
                AWB,
                RUNSHEET_NO,
                SERVICE,
                INBOUND_MANIFEST,
                HVI_NO,
                CASE 
                    WHEN INBOUND_MANIFEST IS NULL OR TRIM(CAST(INBOUND_MANIFEST AS VARCHAR)) = '' OR UPPER(INBOUND_MANIFEST) NOT LIKE 'MXG%' THEN NULL
                    ELSE 
                        GREATEST(
                            CASE WHEN UPPER(INBOUND_MANIFEST) LIKE 'MXG%' AND INBOUND_MANIFEST_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END,
                            CASE WHEN UPPER(HVI_NO) LIKE 'MXG%' AND HVI_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END,
                            CASE WHEN UPPER(MANIFEST_INBOUND_SUBAGEN_NO) LIKE 'MXG%' AND MANIFEST_INBOUND_SUBAGEN_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END
                        )
                END AS valid_inbound_date
            FROM read_parquet('{LOCAL_PARQUET_PATH}')
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

  # --- TABEL DETAIL AWB UNRUNSHEET DENGAN FILTER & DOWNLOAD ---
  st.subheader(f"📋 Detail AWB UnRunsheet — Kategori: [{selected_aging_group}]")

  aging_where_clause = ""
  if selected_aging_group != "Semua Aging":
    if selected_aging_group == "H+0":
      aging_where_clause = (
          "AND DATEDIFF('day', tanggal_max_inbound, CURRENT_TIMESTAMP) <= 0"
      )
    elif selected_aging_group == "H+1":
      aging_where_clause = (
          "AND DATEDIFF('day', tanggal_max_inbound, CURRENT_TIMESTAMP) = 1"
      )
    elif selected_aging_group == "H+2":
      aging_where_clause = (
          "AND DATEDIFF('day', tanggal_max_inbound, CURRENT_TIMESTAMP) = 2"
      )
    elif selected_aging_group == "H+3":
      aging_where_clause = (
          "AND DATEDIFF('day', tanggal_max_inbound, CURRENT_TIMESTAMP) = 3"
      )
    elif selected_aging_group == "H+4":
      aging_where_clause = (
          "AND DATEDIFF('day', tanggal_max_inbound, CURRENT_TIMESTAMP) = 4"
      )
    elif selected_aging_group == "H+5":
      aging_where_clause = (
          "AND DATEDIFF('day', tanggal_max_inbound, CURRENT_TIMESTAMP) = 5"
      )
    elif selected_aging_group == "H+6":
      aging_where_clause = (
          "AND DATEDIFF('day', tanggal_max_inbound, CURRENT_TIMESTAMP) = 6"
      )
    elif selected_aging_group == "H+7":
      aging_where_clause = (
          "AND DATEDIFF('day', tanggal_max_inbound, CURRENT_TIMESTAMP) = 7"
      )
    elif selected_aging_group == ">H+7":
      aging_where_clause = (
          "AND DATEDIFF('day', tanggal_max_inbound, CURRENT_TIMESTAMP) > 7"
      )

  unrunsheet_detail_query = f"""
        WITH processed_data AS (
            SELECT 
                AWB,
                SERVICE,
                RUNSHEET_NO,
                INBOUND_MANIFEST,
                HVI_NO,
                CASE 
                    WHEN INBOUND_MANIFEST IS NULL OR TRIM(CAST(INBOUND_MANIFEST AS VARCHAR)) = '' OR UPPER(INBOUND_MANIFEST) NOT LIKE 'MXG%' THEN NULL
                    ELSE 
                        GREATEST(
                            CASE WHEN UPPER(INBOUND_MANIFEST) LIKE 'MXG%' AND INBOUND_MANIFEST_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END,
                            CASE WHEN UPPER(HVI_NO) LIKE 'MXG%' AND HVI_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END,
                            CASE WHEN UPPER(MANIFEST_INBOUND_SUBAGEN_NO) LIKE 'MXG%' AND MANIFEST_INBOUND_SUBAGEN_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END
                        )
                END AS tanggal_max_inbound
            FROM read_parquet('{LOCAL_PARQUET_PATH}')
        )
        SELECT 
            AWB,
            tanggal_max_inbound AS Tanggal_Max_Inbound,
            CAST(DATEDIFF('day', tanggal_max_inbound, CURRENT_TIMESTAMP) AS VARCHAR) || ' Hari' AS Aging,
            SERVICE
        FROM processed_data
        WHERE (RUNSHEET_NO IS NULL OR TRIM(CAST(RUNSHEET_NO AS VARCHAR)) = '' OR RUNSHEET_NO = '-')
          AND tanggal_max_inbound IS NOT NULL
        {aging_where_clause}
        ORDER BY tanggal_max_inbound ASC
    """

  df_unrunsheet_detail = conn.execute(unrunsheet_detail_query).fetchdf()

  # Tampilkan Tabel Detail
  st.dataframe(df_unrunsheet_detail, use_container_width=True)

  # Fitur Download CSV
  csv_data = df_unrunsheet_detail.to_csv(index=False).encode("utf-8")
  st.download_button(
      label=f"📥 Download Data CSV UnRunsheet [{selected_aging_group}]",
      data=csv_data,
      file_name=f"UnRunsheet_{selected_aging_group.replace(' ', '_')}.csv",
      mime="text/csv",
  )

  st.markdown("---")

  # --- TABEL RINCIAN DATA GABUNGAN UMUM (SAMPLE 100 ROW) ---
  st.subheader("🔍 Detail Data Gabungan Seluruh Bulan (Sample 100 Row)")

  detail_query = f"""
        SELECT 
            AWB,
            ID_ACCOUNT,
            SHIPPER_NAME,
            CONSIGNEE_NAME,
            ORIGIN,
            DEST,
            SERVICE,
            INBOUND_MANIFEST,
            INBOUND_MANIFEST_DATE,
            HVI_NO,
            HVI_DATE,
            MANIFEST_INBOUND_SUBAGEN_NO,
            MANIFEST_INBOUND_SUBAGEN_DATE,
            CASE 
                WHEN INBOUND_MANIFEST IS NULL OR TRIM(CAST(INBOUND_MANIFEST AS VARCHAR)) = '' OR UPPER(INBOUND_MANIFEST) NOT LIKE 'MXG%' THEN NULL
                ELSE 
                    GREATEST(
                        CASE WHEN UPPER(INBOUND_MANIFEST) LIKE 'MXG%' AND INBOUND_MANIFEST_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END,
                        CASE WHEN UPPER(HVI_NO) LIKE 'MXG%' AND HVI_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END,
                        CASE WHEN UPPER(MANIFEST_INBOUND_SUBAGEN_NO) LIKE 'MXG%' AND MANIFEST_INBOUND_SUBAGEN_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END
                    )
            END AS Valid_Inbound_Date,
            CASE 
                WHEN (
                    CASE 
                        WHEN INBOUND_MANIFEST IS NULL OR TRIM(CAST(INBOUND_MANIFEST AS VARCHAR)) = '' OR UPPER(INBOUND_MANIFEST) NOT LIKE 'MXG%' THEN NULL
                        ELSE 
                            GREATEST(
                                CASE WHEN UPPER(INBOUND_MANIFEST) LIKE 'MXG%' AND INBOUND_MANIFEST_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END,
                                CASE WHEN UPPER(HVI_NO) LIKE 'MXG%' AND HVI_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END,
                                CASE WHEN UPPER(MANIFEST_INBOUND_SUBAGEN_NO) LIKE 'MXG%' AND MANIFEST_INBOUND_SUBAGEN_DATE IS NOT NULL THEN TRY_CAST(STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M') AS TIMESTAMP) ELSE NULL END
                            )
                    END
                ) IS NULL THEN 'UnInbound'
                ELSE 'Inbound Valid'
            END AS Status_Inbound,
            COALESCE(RUNSHEET_NO, '-') AS RUNSHEET_NO,
            CASE 
                WHEN RUNSHEET_NO IS NULL OR TRIM(CAST(RUNSHEET_NO AS VARCHAR)) = '' OR RUNSHEET_NO = '-' THEN '🚨 UnRunsheet'
                ELSE 'Runsheeted'
            END AS Status_Runsheet
        FROM read_parquet('{LOCAL_PARQUET_PATH}')
        LIMIT 100
    """

  df_result = conn.execute(detail_query).fetchdf()
  st.dataframe(df_result, use_container_width=True)

  # --- EXPANDER TABEL PRATINJAU PARQUET ---
  with st.expander("📄 Lihat Pratinjau Data Mentah Parquet (50 Row)"):
    st.dataframe(
        conn.execute(
            f"SELECT * FROM read_parquet('{LOCAL_PARQUET_PATH}') LIMIT 50"
        ).fetchdf(),
        use_container_width=True,
    )

except Exception as e:
  st.error(f"Terjadi kesalahan saat memproses data Parquet: {e}")
