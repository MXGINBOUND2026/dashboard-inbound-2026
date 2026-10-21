import datetime
import io
import os
import duckdb
import pandas as pd
import pydeck as pdk
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

try:
  conn = duckdb.connect()

  tab1, tab2, tab3 = st.tabs(
      ["📊 Main Dashboard", "🔒 Summary Exec", "📍 Map UnRunsheet"]
  )

  # =========================================================
  # FILTER GLOBAL SIDEBAR
  # =========================================================
  st.sidebar.markdown("---")
  st.sidebar.header("🔍 Filter Data Dashboard")

  min_max_date = conn.execute(f"""
        SELECT MIN(CAST(Last_Inbound AS DATE)) as min_d, MAX(CAST(Last_Inbound AS DATE)) as max_d 
        FROM read_parquet('{LOCAL_PARQUET_PATH}') WHERE Last_Inbound IS NOT NULL
    """).fetchdf()

  default_start = (
      min_max_date["min_d"][0]
      if not min_max_date.empty and pd.notnull(min_max_date["min_d"][0])
      else datetime.date(2026, 1, 1)
  )
  default_end = (
      min_max_date["max_d"][0]
      if not min_max_date.empty and pd.notnull(min_max_date["max_d"][0])
      else datetime.date.today()
  )

  use_date_filter = st.sidebar.checkbox(
      "📅 Aktifkan Filter Tanggal Last Inbound", value=False
  )

  date_range = None
  if use_date_filter:
    date_range = st.sidebar.date_input(
        "Pilih Rentang Tanggal:",
        value=(default_start, default_end),
        min_value=default_start,
        max_value=default_end,
    )

  unique_locations = [
      r[0]
      for r in conn.execute(
          f"SELECT DISTINCT LAST_LOCATION FROM read_parquet('{LOCAL_PARQUET_PATH}')"
          " WHERE LAST_LOCATION IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]

  allowed_cabang_list = [
      "Malang",
      "Batu",
      "Blitar",
      "Kepanjen",
      "Bululawang",
      "Turen",
      "Unknown",
  ]
  unique_cabang_db = [
      r[0]
      for r in conn.execute(
          f"SELECT DISTINCT Cabang FROM read_parquet('{LOCAL_PARQUET_PATH}')"
          " WHERE Cabang IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]
  unique_cabang = [
      c
      for c in unique_cabang_db
      if any(
          allowed.lower() in str(c).lower() for allowed in allowed_cabang_list
      )
  ]

  unique_zona = [
      r[0]
      for r in conn.execute(
          f"SELECT DISTINCT Zona FROM read_parquet('{LOCAL_PARQUET_PATH}')"
          " WHERE Zona IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]
  unique_kecamatan = [
      r[0]
      for r in conn.execute(
          f"SELECT DISTINCT Kecamatan FROM read_parquet('{LOCAL_PARQUET_PATH}')"
          " WHERE Kecamatan IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]
  unique_status = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT Status_Shipment FROM"
          f" read_parquet('{LOCAL_PARQUET_PATH}') WHERE Status_Shipment IS NOT"
          " NULL ORDER BY 1"
      ).fetchall()
  ]
  unique_hrs = [
      r[0]
      for r in conn.execute(
          f"SELECT DISTINCT HRS FROM read_parquet('{LOCAL_PARQUET_PATH}')"
          " WHERE HRS IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]

  chosen_loc = st.sidebar.multiselect(
      "🔍 Filter Last Location:", unique_locations
  )
  chosen_cabang = st.sidebar.multiselect("🏢 Filter Cabang:", unique_cabang)
  chosen_zona = st.sidebar.multiselect("⚡ Filter Zona (QCE):", unique_zona)
  chosen_kecamatan = st.sidebar.multiselect(
      "📍 Filter Kecamatan:", unique_kecamatan
  )
  chosen_status = st.sidebar.multiselect(
      "📦 Filter Shipment Status:", unique_status
  )
  chosen_hrs = st.sidebar.multiselect("📋 Filter Status HRS:", unique_hrs)

  where_clauses = []
  if use_date_filter and isinstance(date_range, tuple) and len(date_range) == 2:
    start_d, end_d = date_range
    where_clauses.append(
        f"CAST(Last_Inbound AS DATE) BETWEEN '{start_d}' AND '{end_d}'"
    )

  if chosen_loc:
    loc_str = "','".join(chosen_loc)
    where_clauses.append(f"LAST_LOCATION IN ('{loc_str}')")
  if chosen_cabang:
    cab_str = "','".join(chosen_cabang)
    where_clauses.append(f"Cabang IN ('{cab_str}')")
  if chosen_zona:
    zon_str = "','".join(chosen_zona)
    where_clauses.append(f"Zona IN ('{zon_str}')")
  if chosen_kecamatan:
    kec_str = "','".join(chosen_kecamatan)
    where_clauses.append(f"Kecamatan IN ('{kec_str}')")
  if chosen_status:
    stat_str = "','".join(chosen_status)
    where_clauses.append(f"Status_Shipment IN ('{stat_str}')")
  if chosen_hrs:
    hrs_str = "','".join(chosen_hrs)
    where_clauses.append(f"HRS IN ('{hrs_str}')")

  where_sql = (
      "WHERE " + " AND ".join(where_clauses) if where_clauses else "WHERE 1=1"
  )

  # =========================================================
  # TAB 1: MAIN DASHBOARD
  # =========================================================
  with tab1:
    try:
      max_date_df = conn.execute(f"""
                SELECT MAX(
                    COALESCE(
                        TRY_STRPTIME(TGL_TARIK_REPORT, '%m/%d/%Y %H:%M'), 
                        TRY_STRPTIME(TGL_TARIK_REPORT, '%d-%b-%Y %H:%M'), 
                        TRY_STRPTIME(TGL_TARIK_REPORT, '%Y-%m-%d %H:%M'), 
                        TRY_CAST(TGL_TARIK_REPORT AS TIMESTAMP)
                    )
                ) as max_dt FROM read_parquet('{LOCAL_PARQUET_PATH}')
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
            FROM read_parquet('{LOCAL_PARQUET_PATH}') {where_sql} 
            AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')
            GROUP BY Kecamatan ORDER BY total DESC LIMIT 5
        """).fetchdf()

    top_kecamatan_str = " &nbsp;&nbsp;&bull;&nbsp;&nbsp; ".join([
        f"{row.Kecamatan} ({row.total:,})" for idx, row in top_kec_df.iterrows()
    ])
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
    if "active_card_filter" not in st.session_state:
      st.session_state.active_card_filter = None

    metrics_res = conn.execute(f"""
            SELECT 
                COUNT(DISTINCT AWB) AS total_awb,
                COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnInbound' THEN AWB END) AS total_uninbound,
                COUNT(DISTINCT CASE WHEN Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA') THEN AWB END) AS total_unrunsheet,
                COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnRunsheet with PRA' THEN AWB END) AS total_unrunsheet_pra
            FROM read_parquet('{LOCAL_PARQUET_PATH}') {where_sql}
        """).fetchdf()

    total_awb = metrics_res["total_awb"][0] or 0
    total_uninbound = metrics_res["total_uninbound"][0] or 0
    total_unrunsheet = metrics_res["total_unrunsheet"][0] or 0
    total_unrunsheet_pra = metrics_res["total_unrunsheet_pra"][0] or 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
      st.metric(label="📦 Total AWB", value=f"{total_awb:,}")
    with c2:
      st.metric(label="⚠️ Total UnInbound", value=f"{total_uninbound:,}")
      if st.button(
          "👉 Lihat Data UnInbound",
          key="btn_uninbound",
          use_container_width=True,
      ):
        st.session_state.active_view = "UnInbound"
        st.session_state.active_card_filter = "UnInbound"
        st.rerun()
    with c3:
      st.metric(
          label="🚨 Alert: Total UnRunsheet", value=f"{total_unrunsheet:,}"
      )
      if st.button(
          "👉 Lihat Data UnRunsheet",
          key="btn_unrunsheet",
          use_container_width=True,
      ):
        st.session_state.active_view = "UnRunsheet"
        st.session_state.active_card_filter = "Total UnRunsheet"
        st.rerun()
    with c4:
      st.metric(
          label="🚚 UnRunsheet with PRA", value=f"{total_unrunsheet_pra:,}"
      )
      if st.button(
          "👉 Lihat UnRunsheet PRA",
          key="btn_unrunsheet_pra",
          use_container_width=True,
      ):
        st.session_state.active_view = "UnRunsheet PRA"
        st.session_state.active_card_filter = "UnRunsheet with PRA"
        st.rerun()

    st.markdown("---")

    metrics_custom = conn.execute(f"""
            SELECT 
                COUNT(DISTINCT CASE 
                    WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') 
                         AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) 
                    THEN AWB END) AS total_success_unhrs,
                COUNT(DISTINCT CASE 
                    WHEN Status_Shipment = 'UNDELIVERY' 
                         AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) 
                    THEN AWB END) AS total_undel_unhrs
            FROM read_parquet('{LOCAL_PARQUET_PATH}') {where_sql}
        """).fetchdf()

    total_success_unhrs = metrics_custom["total_success_unhrs"][0] or 0
    total_undel_unhrs = metrics_custom["total_undel_unhrs"][0] or 0

    col_m1, col_m2 = st.columns(2)
    with col_m1:
      st.metric(label="⚠️ Success UnHRS", value=f"{total_success_unhrs:,}")
      if st.button(
          "📋 Lihat Detail Success UnHRS",
          key="btn_success_unhrs",
          use_container_width=True,
      ):
        st.session_state.active_card_filter = "Success UnHRS"
        st.rerun()
    with col_m2:
      st.metric(label="❌ Undel UnHRS", value=f"{total_undel_unhrs:,}")
      if st.button(
          "📋 Lihat Detail Undel UnHRS",
          key="btn_undel_unhrs",
          use_container_width=True,
      ):
        st.session_state.active_card_filter = "Undel UnHRS"
        st.rerun()

    if st.session_state.active_card_filter:
      if st.button(
          f"🔄 Reset Filter Card [{st.session_state.active_card_filter}]"
      ):
        st.session_state.active_card_filter = None
        st.rerun()

    st.markdown("---")

    if st.session_state.active_card_filter:
      card_title = st.session_state.active_card_filter
      st.subheader(
          f"📋 Tabel Detail AWB — Berdasarkan Klik Card: [{card_title}]"
      )

      if card_title == "Success UnHRS":
        card_query_filter = """
                    Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') 
                    AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)
                """
      elif card_title == "Undel UnHRS":
        card_query_filter = """
                    Status_Shipment = 'UNDELIVERY' 
                    AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)
                """
      elif card_title == "Total UnRunsheet":
        card_query_filter = (
            "Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')"
        )
      elif card_title == "UnRunsheet with PRA":
        card_query_filter = "Status_Shipment = 'UnRunsheet with PRA'"
      elif card_title == "UnInbound":
        card_query_filter = "Status_Shipment = 'UnInbound'"
      else:
        card_query_filter = "1=1"

      df_card_detail = conn.execute(f"""
                SELECT AWB, Last_Inbound, age_days AS "Aging (Hari)", LAST_LOCATION, Cabang, Zona, Kecamatan, SERVICE, Status_Shipment, HRS, POD_Category, POD_DESC, WUS_REMARKS
                FROM read_parquet('{LOCAL_PARQUET_PATH}')
                {where_sql} AND {card_query_filter}
                ORDER BY age_days DESC
                LIMIT 1000
            """).fetchdf()

      if not df_card_detail.empty:
        st.caption(
            f"Total AWB Ditampilkan: **{len(df_card_detail):,}** (Maksimal 1.000"
            " baris)"
        )
        st.dataframe(df_card_detail, use_container_width=True, height=400)

        csv_card = df_card_detail.to_csv(index=False).encode("utf-8")
        st.download_button(
            label=f"📥 Download Data Detail [{card_title}] (CSV)",
            data=csv_card,
            file_name=f"Detail_Card_{card_title.replace(' ', '_')}.csv",
            mime="text/csv",
        )
      else:
        st.info(
            f"Tidak ada data AWB yang cocok untuk kategori [{card_title}]"
            " dengan filter aktif."
        )

      st.markdown("---")

    view_choice = st.radio(
        "📌 Pilih Tampilan Data Dashboard:",
        ["📊 Analisis UnRunsheet", "⚠️ Tabel UnInbound"],
        horizontal=True,
        index=1 if st.session_state.active_view == "UnInbound" else 0,
    )

    if view_choice == "📊 Analisis UnRunsheet":
      st.subheader(
          "📊 Distribusi Usia Data UnRunsheet (Aging dari Last_Inbound ke Hari"
          " Ini)"
      )

      unrunsheet_status_clause = (
          ""
          if chosen_status
          else "AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')"
      )

      df_aging = conn.execute(f"""
                SELECT aging_group, COUNT(DISTINCT AWB) AS total_awb
                FROM read_parquet('{LOCAL_PARQUET_PATH}')
                {where_sql} {unrunsheet_status_clause}
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
                  categoryorder="array",
                  categoryarray=AGING_ORDER,
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

      aging_clause = (
          f"AND aging_group = '{selected_aging_group}'"
          if selected_aging_group != "Semua Aging"
          else ""
      )

      st.markdown("---")

      st.subheader(
          "📊 Tabel Matrix Status Shipment (Filter Aging:"
          f" [{selected_aging_group}])"
      )

      pivot_matrix = conn.execute(f"""
                PIVOT (
                    SELECT Status_Shipment, aging_group, AWB 
                    FROM read_parquet('{LOCAL_PARQUET_PATH}') {where_sql} {aging_clause}
                )
                ON aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6', 'H+7', '>H+7')
                USING COUNT(DISTINCT AWB)
                GROUP BY Status_Shipment
            """).fetchdf()

      if not pivot_matrix.empty:
        pivot_matrix = pivot_matrix.fillna(0)
        numeric_cols = [
            c for c in pivot_matrix.columns if c != "Status_Shipment"
        ]
        pivot_matrix["Grand Total"] = pivot_matrix[numeric_cols].sum(axis=1)
        st.dataframe(pivot_matrix, use_container_width=True)
      else:
        st.info("Tidak ada data untuk kombinasi filter yang dipilih.")

      st.markdown("---")

      st.subheader(
          "🗂️ Unrunsheet By Last Location (Filter Aging:"
          f" [{selected_aging_group}])"
      )

      pivot_loc = conn.execute(f"""
                PIVOT (
                    SELECT LAST_LOCATION, Cabang, Zona, Kecamatan, aging_group, AWB 
                    FROM read_parquet('{LOCAL_PARQUET_PATH}')
                    {where_sql} {unrunsheet_status_clause} {aging_clause}
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

      st.subheader(
          f"📋 Detail AWB — Kategori Aging: [{selected_aging_group}]"
      )

      df_detail = conn.execute(f"""
                SELECT AWB, Last_Inbound, age_days AS "Aging (Hari)", LAST_LOCATION, Kecamatan, SERVICE, Status_Shipment, HRS, POD_Category, POD_DESC, WUS_REMARKS
                FROM read_parquet('{LOCAL_PARQUET_PATH}')
                {where_sql} {unrunsheet_status_clause} {aging_clause}
                ORDER BY age_days DESC
                LIMIT 1000
            """).fetchdf()

      st.dataframe(df_detail, use_container_width=True)

    else:
      st.subheader(
          "⚠️ Tabel UnInbound (Last Inbound Kosong & Tanpa Runsheet MXG)"
      )
      df_uninbound_show = conn.execute(f"""
                SELECT AWB, Cabang, Zona, Kecamatan, age_days AS "Aging (Hari)", SERVICE, Status_Shipment, HRS
                FROM read_parquet('{LOCAL_PARQUET_PATH}')
                {where_sql} AND Status_Shipment = 'UnInbound'
                ORDER BY age_days DESC
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

  # =========================================================
  # TAB 2: SUMMARY EXEC
  # =========================================================
  with tab2:
    if summary_pass == "MXG125":
      st.title("📊 Executive Summary & Performance Analytics")

      kpi_data = conn.execute(f"""
                SELECT 
                    COUNT(DISTINCT AWB) as total_ytd,
                    COUNT(DISTINCT CASE WHEN Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA') THEN AWB END) as unrun_total,
                    COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) THEN AWB END) as total_success_unhrs_kpi,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) THEN AWB END) as total_undel_unhrs_kpi
                FROM read_parquet('{LOCAL_PARQUET_PATH}') {where_sql}
            """).fetchdf()

      t_ytd = kpi_data["total_ytd"][0] or 0
      u_tot = kpi_data["unrun_total"][0] or 0
      succ_unhrs_val = kpi_data["total_success_unhrs_kpi"][0] or 0
      undel_unhrs_val = kpi_data["total_undel_unhrs_kpi"][0] or 0

      k1, k2, k3, k4 = st.columns(4)
      with k1:
        st.metric(
            label="📦 Total Inbound YTD",
            value=f"{t_ytd:,}",
            delta="Filtered",
        )
      with k2:
        st.metric(
            label="🚨 Total UnRunsheet Aging",
            value=f"{u_tot:,}",
            delta="Active Alert",
            delta_color="inverse",
        )
      with k3:
        st.metric(
            label="⚠️ Total Success UnHRS",
            value=f"{succ_unhrs_val:,}",
            delta="Optimized",
        )
      with k4:
        st.metric(
            label="❌ Total Undel UnHRS",
            value=f"{undel_unhrs_val:,}",
            delta="Needs Review",
            delta_color="inverse",
        )

      st.markdown("---")

      cab_counts = conn.execute(f"""
                SELECT Cabang, COUNT(DISTINCT AWB) as total
                FROM read_parquet('{LOCAL_PARQUET_PATH}')
                {where_sql}
                GROUP BY Cabang
            """).fetchdf()

      summary_str = (
          " &nbsp;&nbsp;&bull;&nbsp;&nbsp; ".join([
              f"<b>{row.Cabang}</b>: {row.total:,} AWB"
              for idx, row in cab_counts.iterrows()
          ])
          if not cab_counts.empty
          else "Tidak ada data sesuai filter"
      )

      st.markdown(
          f"""
            <marquee style="color: #2c3e50; background-color: #e8f4f8; padding: 10px; border-radius: 5px; font-weight: 600; font-size: 15px;">
                🔄 Last Update: {formatted_date} &nbsp;&nbsp;|&nbsp;&nbsp; 🏢 <b>Total Inbound (Filtered):</b> {summary_str}
            </marquee>
        """,
          unsafe_allow_html=True,
      )

      st.markdown("---")

      st.subheader("📈 1. Grafik Total Inbound Per Tanggal (Tahun 2026)")

      col_gran, col_date = st.columns([1, 1])

      with col_gran:
        granularity = st.radio(
            "Pilih Granularitas Waktu:",
            ["Hari", "Week (Minggu)", "Bulan"],
            horizontal=True,
            key="exec_granularity",
        )

      min_max_inbound_2026 = conn.execute(f"""
                SELECT 
                    MIN(CAST(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END) AS DATE)) as min_d, 
                    MAX(CAST(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END) AS DATE)) as max_d 
                FROM read_parquet('{LOCAL_PARQUET_PATH}') 
                {where_sql} AND EXTRACT(YEAR FROM COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END)) = 2026
            """).fetchdf()

      inbound_start = (
          min_max_inbound_2026["min_d"][0]
          if not min_max_inbound_2026.empty
          and pd.notnull(min_max_inbound_2026["min_d"][0])
          else datetime.date(2026, 1, 1)
      )
      inbound_end = (
          min_max_inbound_2026["max_d"][0]
          if not min_max_inbound_2026.empty
          and pd.notnull(min_max_inbound_2026["max_d"][0])
          else datetime.date(2026, 12, 31)
      )

      with col_date:
        inbound_date_range = st.date_input(
            "📅 Filter Rentang Tanggal (Inbound 2026):",
            value=(inbound_start, inbound_end),
            min_value=datetime.date(2026, 1, 1),
            max_value=datetime.date(2026, 12, 31),
            key="exec_date_range",
        )

      inbound_date_clause = ""
      if (
          isinstance(inbound_date_range, tuple)
          and len(inbound_date_range) == 2
      ):
        s_date, e_date = inbound_date_range
        inbound_date_clause = f"AND CAST(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END) AS DATE) BETWEEN '{s_date}' AND '{e_date}'"

      if granularity == "Hari":
        date_expr = (
            "STRFTIME(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, ''))"
            " LIKE '%MXG%' THEN dt_hvi ELSE NULL END), '%d %b')"
        )
      elif granularity == "Week (Minggu)":
        date_expr = (
            "'Week ' || STRFTIME(COALESCE(dt_im, CASE WHEN"
            " UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL"
            " END), '%W')"
        )
      else:
        date_expr = (
            "STRFTIME(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, ''))"
            " LIKE '%MXG%' THEN dt_hvi ELSE NULL END), '%B')"
        )

      df_inbound_trend = conn.execute(f"""
                SELECT 
                    {date_expr} AS periode,
                    COUNT(DISTINCT AWB) AS total_awb,
                    MIN(CAST(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END) AS DATE)) as sort_key
                FROM read_parquet('{LOCAL_PARQUET_PATH}')
                {where_sql} AND EXTRACT(YEAR FROM COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END)) = 2026
                  {inbound_date_clause}
                GROUP BY 1
                ORDER BY sort_key ASC
            """).fetchdf()

      if not df_inbound_trend.empty:
        fig_inbound = go.Figure()
        fig_inbound.add_trace(
            go.Scatter(
                x=df_inbound_trend["periode"],
                y=df_inbound_trend["total_awb"],
                mode="lines+markers",
                fill="tozeroy",
                line=dict(color="#1f77b4", width=3, shape="spline"),
                marker=dict(size=6, color="#1f77b4"),
                fillcolor="rgba(31, 119, 180, 0.3)",
            )
        )
        fig_inbound.update_layout(
            title=dict(
                text="<b>TOTAL INBOUND 2026 (FILTERED)</b>",
                x=0.5,
                xanchor="center",
                font=dict(size=18, color="#FFFFFF"),
            ),
            xaxis=dict(
                showgrid=False,
                zeroline=False,
                tickfont=dict(color="#CCCCCC"),
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="rgba(255, 255, 255, 0.1)",
                gridwidth=1,
                griddash="dash",
                zeroline=False,
                tickfont=dict(color="#CCCCCC"),
            ),
            plot_bgcolor="rgba(15, 32, 67, 0.8)",
            paper_bgcolor="rgba(0, 0, 0, 0)",
            margin=dict(l=40, r=40, t=60, b=40),
            height=350,
        )
        st.plotly_chart(fig_inbound, use_container_width=True)
      else:
        st.info(
            "Tidak ada data Inbound untuk filter dan rentang tanggal tersebut."
        )

      st.markdown("---")

      def render_plotly_table_matrix(df_raw, csv_filename, download_label):
        if df_raw.empty:
          st.info("Tidak ada data.")
          return

        pivot = df_raw.pivot(
            index="Cabang", columns="aging_group", values="total_awb"
        ).fillna(0)
        for col in AGING_ORDER:
          if col not in pivot.columns:
            pivot[col] = 0

        pivot = pivot[AGING_ORDER]
        pivot["Grand Total"] = pivot[AGING_ORDER].sum(axis=1)
        pivot = pivot.sort_values(by="Grand Total", ascending=False)

        grand_total_row = pivot.sum(axis=0)
        grand_total_row.name = "Grand Total"

        pivot_with_total = pd.concat([pivot, pd.DataFrame([grand_total_row])])
        pivot_with_total = pivot_with_total.reset_index().rename(
            columns={"index": "Cabang"}
        )

        headers = list(pivot_with_total.columns)
        cell_values = []
        for col in headers:
          if col == "Cabang":
            cell_values.append(pivot_with_total[col].astype(str).tolist())
          else:
            cell_values.append([
                f"{int(val):,}" for val in pivot_with_total[col].tolist()
            ])

        cell_colors = []
        n_rows = len(pivot_with_total)

        for col in headers:
          col_color_list = []
          for row_idx in range(n_rows):
            if row_idx == n_rows - 1:
              col_color_list.append("#222222")
            elif col == "Cabang" or col == "Grand Total":
              col_color_list.append("#2a2a2a")
            else:
              val = pivot_with_total[col].iloc[row_idx]
              if col == "H+0":
                col_color_list.append("#28a745" if val > 0 else "#1e1e1e")
              elif col in ["H+1", "H+2"]:
                col_color_list.append("#ffc107" if val > 0 else "#1e1e1e")
              elif col in ["H+3", "H+4", "H+5", "H+6", "H+7", ">H+7"]:
                col_color_list.append("#dc3545" if val > 0 else "#1e1e1e")
              else:
                col_color_list.append("#1e1e1e")
          cell_colors.append(col_color_list)

        font_colors = []
        for col in headers:
          col_font_list = []
          for row_idx in range(n_rows):
            if row_idx == n_rows - 1:
              col_font_list.append("#ffffff")
            elif col in ["H+1", "H+2"]:
              val = pivot_with_total[col].iloc[row_idx]
              col_font_list.append("#000000" if val > 0 else "#ffffff")
            else:
              col_font_list.append("#ffffff")
          font_colors.append(col_font_list)

        fig_table = go.Figure(
            data=[
                go.Table(
                    header=dict(
                        values=[f"<b>{h}</b>" for h in headers],
                        fill_color="#333333",
                        align="center",
                        font=dict(color="white", size=13),
                        height=32,
                    ),
                    cells=dict(
                        values=cell_values,
                        fill_color=cell_colors,
                        align=[
                            "left" if h == "Cabang" else "center"
                            for h in headers
                        ],
                        font=dict(color=font_colors, size=12),
                        height=28,
                    ),
                )
            ]
        )

        fig_table.update_layout(
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=280,
        )

        st.plotly_chart(fig_table, use_container_width=True)

        csv_data = pivot_with_total.to_csv(index=False).encode("utf-8")
        st.download_button(
            label=download_label,
            data=csv_data,
            file_name=csv_filename,
            mime="text/csv",
        )

      st.subheader(
          "📊 2. Matrix Infografis UnRunsheet Per Cabang (Aging Last_Inbound)"
      )
      df_unrun = conn.execute(f"""
                SELECT Cabang, aging_group, COUNT(DISTINCT AWB) AS total_awb 
                FROM read_parquet('{LOCAL_PARQUET_PATH}')
                {where_sql} AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')
                GROUP BY Cabang, aging_group
            """).fetchdf()
      render_plotly_table_matrix(
          df_unrun,
          "Matrix_UnRunsheet_Cabang.csv",
          "📥 Download Data Matrix UnRunsheet (CSV)",
      )

      st.markdown("---")

      st.subheader(
          "📊 3. Matrix Infografis Success UnHRS Per Cabang (Aging"
          " TGL_UPDATE_STATUS_POD)"
      )
      df_succ = conn.execute(f"""
                SELECT Cabang, aging_group, COUNT(DISTINCT AWB) AS total_awb
                FROM (
                    SELECT Cabang, 
                        CASE 
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) <= 0 THEN 'H+0'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 1 THEN 'H+1'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 2 THEN 'H+2'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 3 THEN 'H+3'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 4 THEN 'H+4'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 5 THEN 'H+5'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 6 THEN 'H+6'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 7 THEN 'H+7'
                            ELSE '>H+7'
                        END AS aging_group, AWB
                    FROM read_parquet('{LOCAL_PARQUET_PATH}')
                    {where_sql} AND Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER')
                      AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)
                ) sub GROUP BY Cabang, aging_group
            """).fetchdf()
      render_plotly_table_matrix(
          df_succ,
          "Matrix_Success_UnHRS_Cabang.csv",
          "📥 Download Data Matrix Success UnHRS (CSV)",
      )

      st.markdown("---")

      st.subheader(
          "📊 4. Matrix Infografis Undel UnHRS Per Cabang (Aging"
          " TGL_UPDATE_STATUS_POD)"
      )
      df_undel = conn.execute(f"""
                SELECT Cabang, aging_group, COUNT(DISTINCT AWB) AS total_awb
                FROM (
                    SELECT Cabang, 
                        CASE 
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) <= 0 THEN 'H+0'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 1 THEN 'H+1'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 2 THEN 'H+2'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 3 THEN 'H+3'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 4 THEN 'H+4'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 5 THEN 'H+5'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 6 THEN 'H+6'
                            WHEN DATEDIFF('day', TGL_UPDATE_POD, CURRENT_TIMESTAMP) = 7 THEN 'H+7'
                            ELSE '>H+7'
                        END AS aging_group, AWB
                    FROM read_parquet('{LOCAL_PARQUET_PATH}')
                    {where_sql} AND Status_Shipment = 'UNDELIVERY'
                      AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)
                ) sub GROUP BY Cabang, aging_group
            """).fetchdf()
      render_plotly_table_matrix(
          df_undel,
          "Matrix_Undel_UnHRS_Cabang.csv",
          "📥 Download Data Matrix Undel UnHRS (CSV)",
      )

    else:
      st.warning(
          "🔒 Halaman Summary Exec Terkunci. Masukkan password **MXG125** pada"
          " sidebar di sebelah kiri untuk membuka laporan."
      )

  # =========================================================
  # TAB 3: MAP UNRUNSHEET
  # =========================================================
  with tab3:
    st.subheader(
        "📍 Peta Interaktif Sebaran UnRunsheet per Kecamatan (Malang Raya &"
        " Blitar Raya)"
    )
    st.caption(
        "Visualisasi titik koordinat wilayah kecamatan UnRunsheet yang"
        " dioptimalkan agar cepat termuat."
    )

    df_map_raw = conn.execute(f"""
            SELECT UPPER(TRIM(Kecamatan)) AS Kecamatan, COUNT(DISTINCT AWB) AS total_unrun
            FROM read_parquet('{LOCAL_PARQUET_PATH}')
            {where_sql} AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')
              AND Kecamatan IS NOT NULL
            GROUP BY 1
            ORDER BY total_unrun DESC
        """).fetchdf()

    if not df_map_raw.empty:
      coords_map = {
          "MALANG": [-7.9666, 112.6326],
          "KEPANJEN": [-8.1308, 112.5693],
          "BATU": [-7.8767, 112.5234],
          "BLITAR": [-8.0954, 112.1627],
          "TUREN": [-8.1691, 112.6953],
          "BULULAWANG": [-8.0475, 112.6394],
          "BLIMBING": [-7.9546, 112.6481],
          "LOWOKWARU": [-7.9463, 112.6155],
          "KLOJEN": [-7.9785, 112.6316],
          "SUKUN": [-8.0003, 112.6183],
          "KEDUNGKANDANG": [-7.9942, 112.6568],
          "PAKIS": [-7.9528, 112.6931],
          "DAMPIT": [-8.2323, 112.7486],
      }

      lat_list, lon_list, size_list = [], [], []
      for idx, row in df_map_raw.iterrows():
        kec = row["Kecamatan"]
        if kec in coords_map:
          lat_list.append(coords_map[kec][0])
          lon_list.append(coords_map[kec][1])
        else:
          lat_list.append(-7.9839 + (((idx * 3) % 15) * 0.012))
          lon_list.append(112.6214 + (((idx * 3) % 15) * 0.012))

        size_list.append(max(float(row["total_unrun"]) * 8, 2500))

      df_map_raw["lat"] = lat_list
      df_map_raw["lon"] = lon_list
      df_map_raw["radius"] = size_list

      layer = pdk.Layer(
          "ScatterplotLayer",
          data=df_map_raw,
          get_position=["lon", "lat"],
          get_color=[220, 53, 69, 190],
          get_radius="radius",
          pickable=True,
          auto_highlight=True,
      )

      view_state = pdk.ViewState(
          latitude=-7.9839, longitude=112.6214, zoom=10.5, pitch=25
      )

      r = pdk.Deck(
          layers=[layer],
          initial_view_state=view_state,
          tooltip={
              "html": "<b>Kecamatan:</b> {Kecamatan}<br/><b>Total UnRunsheet:</b> {total_unrun}",
              "style": {
                  "backgroundColor": "#1f2937",
                  "color": "white",
                  "borderRadius": "6px",
                  "padding": "8px",
              },
          },
          map_style="mapbox://styles/mapbox/dark-v10",
      )

      st.pydeck_chart(r, use_container_width=True)

      st.markdown("### 📋 Rangkuman Data UnRunsheet per Kecamatan")
      st.dataframe(
          df_map_raw[["Kecamatan", "total_unrun"]],
          use_container_width=True,
          height=300,
      )
    else:
      st.info(
          "Tidak ada data geolokasi UnRunsheet untuk ditampilkan berdasarkan"
          " filter aktif."
      )

except Exception as main_err:
  st.error("⚠️ Terjadi masalah saat mengolah data dashboard:")
  st.exception(main_err)
