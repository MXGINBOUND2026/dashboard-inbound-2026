import glob
import os
import shutil
import duckdb
import plotly.express as px
import streamlit as st

# Konfigurasi Halaman Dashboard
st.set_page_config(
    page_title="Database Inbound 2026", page_icon="🚚", layout="wide"
)

st.title("🚚 Database Inbound 2026 (Analisis Nasional)")

# URL Folder Utama INBOUND di Google Drive Anda
FOLDER_URL = (
    "https://drive.google.com/drive/folders/1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"
)

download_dir = os.path.join(os.getcwd(), "downloaded_inbound")


# Fungsi untuk Mengunduh Folder dari Google Drive dengan Caching
@st.cache_resource
def download_inbound_folder(folder_url):
  if not os.path.exists(download_dir):
    os.makedirs(download_dir)

  with st.spinner(
      "Sedang menyinkronkan dan mengunduh seluruh data dari Google Drive..."
  ):
    try:
      import gdown

      gdown.download_folder(url=folder_url, output=download_dir, quiet=False)
    except Exception as e:
      st.warning(
          f"Catatan sinkronisasi (menampilkan data lokal yang tersedia): {e}"
      )

  return download_dir


# Sidebar: Tombol Refresh Sinkronisasi GDrive
st.sidebar.header("⚙️ Pengaturan & Sinkronisasi GDrive")
if st.sidebar.button("🔄 Refresh & Tarik Data Terbaru"):
  st.cache_resource.clear()
  if os.path.exists(download_dir) and os.path.isdir(download_dir):
    shutil.rmtree(download_dir, ignore_errors=True)
  st.success("Data di-reset! Mengunduh ulang seluruh data terbaru...")
  st.rerun()

# Jalankan Pengunduhan / Pembacaan Folder
root_dir = download_inbound_folder(FOLDER_URL)

# Buat Koneksi DuckDB
conn = duckdb.connect()

# Pola wildcard (glob) untuk dibaca langsung oleh DuckDB
csv_glob_pattern = os.path.join(root_dir, "**", "*.csv").replace("\\", "/")
csv_files = glob.glob(os.path.join(root_dir, "**", "*.csv"), recursive=True)

if not csv_files:
  st.error(
      "Tidak ditemukan file CSV di dalam folder. Pastikan URL folder Google"
      " Drive benar dan izin akses sudah diatur ke 'Anyone with the link'."
  )
else:
  st.success(
      f"📂 Ditemukan **{len(csv_files)} file CSV**. DuckDB memproses data secara"
      " instan tanpa membebani RAM!"
  )

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
                FROM read_csv_auto('{csv_glob_pattern}', union_by_name=true, ignore_errors=true)
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

    # --- QUERY DISTRIBUSI USIA UNRUNSHEET (MURNI UNRUNSHEET VALID) ---
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
                FROM read_csv_auto('{csv_glob_pattern}', union_by_name=true, ignore_errors=true)
            ),
            unrunsheet_aging AS (
                SELECT 
                    AWB,
                    valid_inbound_date,
                    DATEDIFF('day', valid_inbound_date, CURRENT_TIMESTAMP) AS age_days
                FROM processed_data
                WHERE (RUNSHEET_NO IS NULL OR TRIM(CAST(RUNSHEET_NO AS VARCHAR)) = '' OR RUNSHEET_NO = '-')
                  AND valid_inbound_date IS NOT NULL  -- MENGELUARKAN DATA UNINBOUND
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
        # Menggunakan Plotly Express untuk tampilan persis sesuai sampel gambar
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

    # --- TABEL DETAIL AWB UNRUNSHEET DENGAN FILTER & TOMBOL DOWNLOAD ---
    st.subheader(
        f"📋 Detail AWB UnRunsheet — Kategori: [{selected_aging_group}]"
    )

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
                FROM read_csv_auto('{csv_glob_pattern}', union_by_name=true, ignore_errors=true)
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

    # Tampilkan Tabel
    st.dataframe(df_unrunsheet_detail, use_container_width=True)

    # --- FITUR DOWNLOAD CSV ---
    csv_data = df_unrunsheet_detail.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"📥 Download Data CSV UnRunsheet [{selected_aging_group}]",
        data=csv_data,
        file_name=f"UnRunsheet_{selected_aging_group.replace(' ', '_')}.csv",
        mime="text/csv",
    )

    st.markdown("---")

    # --- TABEL RINCIAN DATA GABUNGAN UMUM ---
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
            FROM read_csv_auto('{csv_glob_pattern}', union_by_name=true, ignore_errors=true)
            LIMIT 100
        """

    df_result = conn.execute(detail_query).fetchdf()
    st.dataframe(df_result, use_container_width=True)

  except Exception as e:
    st.error(f"Terjadi kesalahan saat memproses query SQL: {e}")

  # --- EXPANDER TABEL DATA MENTAH (RAW CSV) ---
  with st.expander("📄 Lihat Pratinjau Data Mentah Gabungan (Raw CSV)"):
    st.dataframe(
        conn.execute(
            f"SELECT * FROM read_csv_auto('{csv_glob_pattern}', union_by_name=true, ignore_errors=true) LIMIT 50"
        ).fetchdf(),
        use_container_width=True,
    )